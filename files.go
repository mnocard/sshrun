package main

import (
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/pkg/sftp"
)

func humanSize(n int64) string {
	const k = 1024
	switch {
	case n < k:
		return fmt.Sprintf("%d Б", n)
	case n < k*k:
		return fmt.Sprintf("%.1f КБ", float64(n)/k)
	case n < k*k*k:
		return fmt.Sprintf("%.1f МБ", float64(n)/(k*k))
	default:
		return fmt.Sprintf("%.2f ГБ", float64(n)/(k*k*k))
	}
}

// resolveDest — по правилам scp: если remote — существующий каталог или заканчивается на «/»,
// файл/каталог кладётся ВНУТРЬ него под своим именем; иначе remote — это конечный путь.
func resolveDest(sc *sftp.Client, local, remote string) string {
	base := filepath.Base(filepath.Clean(local))
	if fi, err := sc.Stat(remote); err == nil && fi.IsDir() {
		return path.Join(remote, base)
	}
	if strings.HasSuffix(remote, "/") {
		return path.Join(remote, base)
	}
	return remote
}

func copyFile(sc *sftp.Client, local, remote string) (int64, error) {
	src, err := os.Open(local)
	if err != nil {
		return 0, err
	}
	defer src.Close()
	_ = sc.MkdirAll(path.Dir(remote))
	dst, err := sc.Create(remote)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", remote, err)
	}
	n, err := io.Copy(dst, src)
	if cerr := dst.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		err = fmt.Errorf("%s: %w", remote, err)
	}
	return n, err
}

func copyTree(sc *sftp.Client, local, remote string, report func(string)) error {
	fi, err := os.Stat(local)
	if err != nil {
		return err
	}
	if !fi.IsDir() {
		n, err := copyFile(sc, local, remote)
		if err == nil {
			report(fmt.Sprintf("↑ %s (%s)", remote, humanSize(n)))
		}
		return err
	}

	total := 0
	filepath.WalkDir(local, func(_ string, d fs.DirEntry, err error) error {
		if err == nil && d.Type().IsRegular() {
			total++
		}
		return nil
	})
	if err := sc.MkdirAll(remote); err != nil {
		return fmt.Errorf("%s: %w", remote, err)
	}
	var files int
	var bytes int64
	err = filepath.WalkDir(local, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(local, p)
		if rel == "." {
			return nil
		}
		rp := path.Join(remote, filepath.ToSlash(rel))
		switch {
		case d.IsDir():
			if err := sc.MkdirAll(rp); err != nil {
				return fmt.Errorf("%s: %w", rp, err)
			}
		case d.Type().IsRegular():
			n, err := copyFile(sc, p, rp)
			if err != nil {
				return err
			}
			files++
			bytes += n
			if total <= 30 || files%50 == 0 {
				report(fmt.Sprintf("↑ %s (%s) [%d/%d]", rp, humanSize(n), files, total))
			}
		default:
			report("пропущен не обычный файл: " + p)
		}
		return nil
	})
	if err == nil {
		report(fmt.Sprintf("каталог загружен: %d файл(ов), %s", files, humanSize(bytes)))
	}
	return err
}

// Upload загружает файл или каталог. При нехватке прав и default_sudo — через
// временный каталог в /tmp и «sudo cp».
func (c *Conn) Upload(local, remote string, report func(string)) error {
	if _, err := os.Stat(local); err != nil {
		return err
	}
	remote = c.expand(remote)
	sc, err := c.sftpc()
	if err != nil {
		return err
	}
	dst := resolveDest(sc, local, remote)
	err = copyTree(sc, local, dst, report)
	if err == nil || !(c.sudo && isPerm(err)) {
		return err
	}

	report("недостаточно прав на запись — загрузка через /tmp и sudo cp")
	fi, _ := os.Stat(local)
	stage := "/tmp/.sshrun-" + randHex(6)
	if err := sc.Mkdir(stage); err != nil {
		return err
	}
	defer sc.RemoveAll(stage)
	item := stage + "/" + filepath.Base(filepath.Clean(local))
	if err := copyTree(sc, local, item, func(string) {}); err != nil {
		return err
	}
	var script string
	if fi.IsDir() {
		script = fmt.Sprintf("mkdir -p %s && cp -r %s/. %s/", shQuote(dst), shQuote(item), shQuote(dst))
	} else {
		script = fmt.Sprintf("mkdir -p %s && cp -f %s %s", shQuote(path.Dir(dst)), shQuote(item), shQuote(dst))
	}
	if _, se, err := c.runSilent("sh -c "+shQuote(script), true); err != nil {
		return fmt.Errorf("sudo cp: %v: %s", err, strings.TrimSpace(se))
	}
	report("загружено в " + dst + " (через sudo, владелец — root)")
	return nil
}

// Replace заменяет текст в файле на сервере. Если искомого нет — это ошибка.
func (c *Conn) Replace(rc ReplaceCfg) (int, error) {
	data, err := c.ReadFile(rc.File)
	if err != nil {
		return 0, err
	}
	s := string(data)
	var n int
	var res string
	if rc.Regex {
		re, err := regexp.Compile(rc.Find)
		if err != nil {
			return 0, fmt.Errorf("regex: %w", err)
		}
		n = len(re.FindAllStringIndex(s, -1))
		res = re.ReplaceAllString(s, rc.Replace)
	} else {
		n = strings.Count(s, rc.Find)
		res = strings.ReplaceAll(s, rc.Find, rc.Replace)
	}
	if n == 0 {
		return 0, fmt.Errorf("в %s не найдено %q", rc.File, rc.Find)
	}
	if err := c.WriteFile(rc.File, []byte(res)); err != nil {
		return 0, err
	}
	return n, nil
}
