package main

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strings"
	"sync/atomic"
	"time"

	"github.com/pkg/sftp"
)

// reporter — куда загрузка сообщает о ходе работы: note — обычные строки журнала,
// progress — периодическое состояние большого файла (в GUI рисуется одной полоской
// на месте, в консоли печатается строкой).
type reporter struct {
	note     func(string)
	progress func(text string, pct int)
}

func (r reporter) Note(s string) {
	if r.note != nil {
		r.note(s)
	}
}

func (r reporter) Progress(text string, pct int) {
	if r.progress != nil {
		r.progress(text, pct)
	}
}

// Ниже порога прогресс не показываем — на пачке мелких файлов он только шумит,
// а сам файл и так загрузится за секунды.
const progressThreshold = 8 * 1024 * 1024

// xferReader читает локальный файл для одной SFTP-передачи: считает прочитанные
// байты атомарно (для прогресса) и проверяет контекст перед каждым чтением —
// это и есть вся реализация отмены: как только ctx отменён, Read начинает
// возвращать ошибку, io.Copy прерывается, Upload возвращает context.Canceled.
// Проверка — поверх уже читаемых данных, поэтому на скорость передачи, пока
// не отменено, не влияет.
//
// Поле f — НЕ встроено (не анонимное): начиная с Go 1.22 у *os.File появился
// метод WriteTo, и если бы мы встроили *os.File, xferReader promoted-бы его —
// тогда io.Copy(dst, xferReader) выбрал бы src.WriteTo(dst) в обход нашего Read
// (WriteTo не вызывает Read), и ни счётчик, ни отмена не работали бы при
// работающей передаче. Явно реализуем только Read и Stat (чтобы sftp.File.
// ReadFrom по-прежнему узнавал размер файла и включал быстрый конкурентный
// путь записи — см. github.com/pkg/sftp, ReadFrom: switch по
// interface{ Stat() (os.FileInfo, error) }).
type xferReader struct {
	f   *os.File
	ctx context.Context
	n   int64
}

func (r *xferReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	n, err := r.f.Read(p)
	if n > 0 {
		atomic.AddInt64(&r.n, int64(n))
	}
	return n, err
}

func (r *xferReader) Stat() (os.FileInfo, error) { return r.f.Stat() }

// progressTicker раз в 2 секунды печатает процент, объём и текущую скорость.
// Работает в отдельной горутине и не влияет на скорость самой передачи.
func progressTicker(cr *xferReader, total int64, label string, report reporter, stop <-chan struct{}) {
	t := time.NewTicker(2 * time.Second)
	defer t.Stop()
	last, lastAt := int64(0), time.Now()
	for {
		select {
		case <-stop:
			return
		case now := <-t.C:
			n := atomic.LoadInt64(&cr.n)
			speed := float64(n-last) / now.Sub(lastAt).Seconds()
			last, lastAt = n, now
			pct := 100
			if total > 0 {
				pct = int(n * 100 / total)
			}
			report.Progress(fmt.Sprintf("%s: %d%% (%s из %s), %s/с", label, pct, humanSize(n), humanSize(total), humanSize(int64(speed))), pct)
		}
	}
}

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

func copyFile(ctx context.Context, sc *sftp.Client, local, remote string, report reporter) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
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

	xr := &xferReader{f: src, ctx: ctx}
	if fi, statErr := src.Stat(); statErr == nil && report.progress != nil && fi.Size() >= progressThreshold {
		stop := make(chan struct{})
		go progressTicker(xr, fi.Size(), remote, report, stop)
		defer close(stop)
	}

	n, err := io.Copy(dst, xr)
	if cerr := dst.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		err = fmt.Errorf("%s: %w", remote, err)
	}
	return n, err
}

func copyTree(ctx context.Context, sc *sftp.Client, local, remote string, report reporter) error {
	fi, err := os.Stat(local)
	if err != nil {
		return err
	}
	if !fi.IsDir() {
		n, err := copyFile(ctx, sc, local, remote, report)
		if err == nil {
			report.Note(fmt.Sprintf("↑ %s (%s)", remote, humanSize(n)))
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
		// Проверяем отмену между файлами — так остановка не ждёт файла, который
		// как раз передаётся, и не начинает следующий после отмены.
		if cerr := ctx.Err(); cerr != nil {
			return cerr
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
			n, err := copyFile(ctx, sc, p, rp, report)
			if err != nil {
				return err
			}
			files++
			bytes += n
			if total <= 30 || files%50 == 0 {
				report.Note(fmt.Sprintf("↑ %s (%s) [%d/%d]", rp, humanSize(n), files, total))
			}
		default:
			report.Note("пропущен не обычный файл: " + p)
		}
		return nil
	})
	if err == nil {
		report.Note(fmt.Sprintf("каталог загружен: %d файл(ов), %s", files, humanSize(bytes)))
	}
	return err
}

// Upload загружает файл или каталог. При нехватке прав и default_sudo — через
// временный каталог в /tmp и «sudo cp». Можно прервать в любой момент —
// Interrupt() (кнопка «Остановить»/команда break) отменяет контекст, которым
// помечена вся загрузка; Upload в этом случае вернёт понятную ошибку, а не
// «зависнет» до конца файла и не продолжит молча после отмены.
func (c *Conn) Upload(local, remote string, report reporter) error {
	if _, err := os.Stat(local); err != nil {
		return err
	}
	ctx, done := c.startUpload(oneLine(local + " -> " + remote))
	defer done()

	remote = c.expand(remote)
	sc, err := c.sftpc()
	if err != nil {
		return err
	}
	dst := resolveDest(sc, local, remote)
	err = copyTree(ctx, sc, local, dst, report)
	if err == nil {
		return nil
	}
	if cerr := ctx.Err(); cerr != nil {
		return fmt.Errorf("загрузка отменена пользователем")
	}
	if !(c.sudo && isPerm(err)) {
		return err
	}

	report.Note("недостаточно прав на запись — загрузка через /tmp и sudo cp")
	fi, _ := os.Stat(local)
	stage := "/tmp/.sshrun-" + randHex(6)
	if err := sc.Mkdir(stage); err != nil {
		return err
	}
	defer sc.RemoveAll(stage)
	item := stage + "/" + filepath.Base(filepath.Clean(local))
	if err := copyTree(ctx, sc, local, item, report); err != nil {
		if cerr := ctx.Err(); cerr != nil {
			return fmt.Errorf("загрузка отменена пользователем")
		}
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
	report.Note("загружено в " + dst + " (через sudo, владелец — root)")
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
