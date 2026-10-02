package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
)

// Conn — одно SSH-подключение: постоянная интерактивная оболочка на PTY
// (состояние cd/env/su сохраняется между командами) + отдельные каналы
// для SFTP и служебных команд.
//
// Команда считается выполненной, когда оболочка «освободила ввод»: после каждой
// команды мы отправляем в тот же поток служебную строку, печатающую уникальный
// маркер с кодом возврата. Пока маркер не пришёл — оболочка занята.
type Conn struct {
	name string
	cfg  ServerCfg
	sudo bool // default_sudo: подставлять пароль в запросы sudo и использовать sudo для файловых операций
	out  *Console

	cli   *ssh.Client
	sess  *ssh.Session
	stdin io.WriteCloser
	wmu   sync.Mutex // сериализует запись в stdin

	mu       sync.Mutex // состояние потока вывода ниже
	acc      string
	waiter   *waiter
	skipNL   bool
	curLine  string
	answered bool

	busySince time.Time // когда началась текущая команда/загрузка (для GUI)
	busyCmd   string    // её краткий текст
	onChange  func()    // вызывается при смене состояния (команда началась/закончилась, связь потеряна)

	// Активная загрузка файла (SFTP) — отдельный от waiter признак «занят»,
	// т.к. идёт по своему каналу и не управляется маркерами завершения команды.
	// uploadCancel ненулевой ровно пока загрузка выполняется; Interrupt() вызывает
	// именно его, если он есть, — так кнопки «Остановить»/команда break прерывают
	// и загрузку файла точно так же, как обычную команду, без отдельного UI.
	uploadCancel context.CancelFunc

	execMu   sync.Mutex // одна команда за раз
	closed   chan struct{}
	closeErr error
	closing  atomic.Bool

	sftpMu sync.Mutex
	sftp   *sftp.Client
	home   string
}

type waiter struct {
	prefix string
	done   chan int
}

// Запрос пароля sudo: «[sudo] password for user:» (в любой локали) или «password for user:» в начале строки.
var sudoPromptRe = regexp.MustCompile(`(?i)(\[sudo\][^:]*:\s*$)|(^password for [^:]+:\s*$)`)

func randHex(n int) string {
	b := make([]byte, n)
	rand.Read(b)
	return hex.EncodeToString(b)
}

func shQuote(s string) string { return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'" }

func authMethods(cfg ServerCfg) ([]ssh.AuthMethod, error) {
	var auth []ssh.AuthMethod
	if cfg.KeyFile != "" {
		pem, err := os.ReadFile(cfg.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("key_file: %w", err)
		}
		var signer ssh.Signer
		if cfg.KeyPass != "" {
			signer, err = ssh.ParsePrivateKeyWithPassphrase(pem, []byte(cfg.KeyPass))
		} else {
			signer, err = ssh.ParsePrivateKey(pem)
		}
		if err != nil {
			return nil, fmt.Errorf("key_file: %w", err)
		}
		auth = append(auth, ssh.PublicKeys(signer))
	}
	if cfg.Password != "" {
		pass := cfg.Password
		auth = append(auth, ssh.Password(pass))
		// Многие серверы с PAM/SSSD/AD принимают пароль только через keyboard-interactive.
		auth = append(auth, ssh.KeyboardInteractive(func(_, _ string, qs []string, _ []bool) ([]string, error) {
			ans := make([]string, len(qs))
			for i := range ans {
				ans[i] = pass
			}
			return ans, nil
		}))
	}
	if len(auth) == 0 {
		return nil, errors.New("не задан ни password, ни key_file")
	}
	return auth, nil
}

func Connect(cfg ServerCfg, timeout time.Duration, sudo bool, out *Console, onChange func()) (*Conn, error) {
	auth, err := authMethods(cfg)
	if err != nil {
		return nil, err
	}
	cc := &ssh.ClientConfig{
		User: cfg.Username,
		Auth: auth,
		// Внутренние серверы, подключение по паролю из конфига. Если нужна проверка
		// отпечатка — замените на ssh.FixedHostKey / knownhosts.New.
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		Timeout:         timeout,
	}
	// Свой Dial вместо ssh.Dial: (1) TCP keepalive на уровне сокета — NAT/файрволы не
	// «забывают» соединение во время многоминутных команд без вывода; (2) connect_timeout
	// ограничивает и рукопожатие с аутентификацией, а не только установку TCP.
	addr := net.JoinHostPort(cfg.Host, strconv.Itoa(cfg.Port))
	d := net.Dialer{Timeout: timeout, KeepAlive: 15 * time.Second}
	nc, err := d.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	nc.SetDeadline(time.Now().Add(timeout))
	sconn, chans, reqs, err := ssh.NewClientConn(nc, addr, cc)
	if err != nil {
		nc.Close()
		return nil, err
	}
	nc.SetDeadline(time.Time{})
	cli := ssh.NewClient(sconn, chans, reqs)
	sess, err := cli.NewSession()
	if err != nil {
		cli.Close()
		return nil, err
	}
	// TERM=dumb: без цветов и прогресс-баров; 250 колонок: длинные строки не переносятся.
	modes := ssh.TerminalModes{ssh.ECHO: 0, ssh.TTY_OP_ISPEED: 115200, ssh.TTY_OP_OSPEED: 115200}
	if err := sess.RequestPty("dumb", 50, 250, modes); err != nil {
		cli.Close()
		return nil, err
	}
	stdin, err := sess.StdinPipe()
	if err != nil {
		cli.Close()
		return nil, err
	}
	stdout, err := sess.StdoutPipe()
	if err != nil {
		cli.Close()
		return nil, err
	}
	// --noediting: без readline, иначе он сам эхо-печатает ввод и «stty -echo» не действует.
	if err := sess.Start(`command -v bash >/dev/null 2>&1 && exec bash --noediting -il || exec sh -i`); err != nil {
		cli.Close()
		return nil, err
	}

	c := &Conn{
		name: cfg.Name, cfg: cfg, sudo: sudo, out: out,
		cli: cli, sess: sess, stdin: stdin,
		closed: make(chan struct{}), onChange: onChange,
	}
	go c.readLoop(stdout)
	go c.keepAlive()

	// Настройка оболочки: пустые приглашения, без эха. Баннер/MOTD в лог пишем, на экран не выводим.
	out.Hide(cfg.Name, true)
	// trap : INT — иначе после Ctrl+C интерактивный bash отменяет всю строку, включая маркер.
	_, err = c.exec("export PS1= PS2= PROMPT_COMMAND=; stty -echo 2>/dev/null; trap : INT; true", timeout)
	out.FlushServer(cfg.Name)
	out.Hide(cfg.Name, false)
	if err != nil {
		c.Close()
		return nil, fmt.Errorf("оболочка не ответила: %w", err)
	}
	return c, nil
}

// Busy — выполняется ли сейчас команда (оболочка ещё не вернула ввод).
// Busy — занято ли соединение чем бы то ни было (для статуса/кнопок в GUI).
func (c *Conn) Busy() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.waiter != nil || c.uploadCancel != nil
}

// ExecBusy — выполняется ли именно shell-команда (не загрузка файла). Отдельно
// от Busy(): во время загрузки PTY-оболочка на самом деле свободна (загрузка
// идёт по своему SFTP-каналу), так что слать туда «ответ на вопрос программы»
// было бы неверно — он просто выполнится как произвольная новая команда в обход
// обычного механизма запуска. Для маршрутизации ручного ввода годится только
// этот, более узкий признак.
func (c *Conn) ExecBusy() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.waiter != nil
}

func (c *Conn) Alive() bool {
	select {
	case <-c.closed:
		return false
	default:
		return true
	}
}

// Параметры keepalive (переменные — чтобы можно было сократить в тестах).
var (
	kaInterval = 15 * time.Second // как часто спрашивать сервер
	kaTimeout  = 30 * time.Second // сколько ждать ответ на один запрос
	kaMaxMiss  = 3                // сколько подряд неотвеченных запросов считаем обрывом связи
)

// keepAlive поддерживает соединение во время долгих команд и заодно замечает обрыв:
// если сервер не отвечает kaMaxMiss раз подряд, соединение закрывается — выполняющаяся
// команда завершается ошибкой (шаг приостанавливается), а не висит бесконечно.
func (c *Conn) keepAlive() {
	t := time.NewTicker(kaInterval)
	defer t.Stop()
	miss := 0
	for {
		select {
		case <-c.closed:
			return
		case <-t.C:
		}
		res := make(chan error, 1)
		go func() {
			_, _, err := c.cli.SendRequest("keepalive@openssh.com", true, nil)
			res <- err
		}()
		select {
		case err := <-res:
			if err != nil {
				return // транспорт уже упал — readLoop сам всё закроет
			}
			miss = 0
		case <-time.After(kaTimeout):
			miss++
			if miss >= kaMaxMiss {
				c.out.Note(c.name, "сервер не отвечает на keepalive (%d раз подряд) — соединение закрыто", miss)
				c.cli.Close()
				return
			}
		case <-c.closed:
			return
		}
	}
}

// changed сообщает наверх, что состояние соединения изменилось.
func (c *Conn) changed() {
	if c.onChange != nil {
		c.onChange()
	}
}

// BusyInfo — выполняется ли команда, с какого момента и какая.
func (c *Conn) BusyInfo() (busy bool, since time.Time, cmd string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.waiter != nil || c.uploadCancel != nil, c.busySince, c.busyCmd
}

// startUpload помечает соединение «занятым» на время загрузки (busySince/busyCmd —
// те же поля, что и для shell-команд, GUI не различает их природу) и возвращает
// контекст, который Interrupt() отменит по запросу пользователя. done — обязательно
// вызвать по завершении (и успешном, и с ошибкой), иначе соединение останется
// видно как «занятое» навсегда.
func (c *Conn) startUpload(label string) (ctx context.Context, done func()) {
	ctx, cancel := context.WithCancel(context.Background())
	c.mu.Lock()
	c.uploadCancel = cancel
	c.busySince, c.busyCmd = time.Now(), label
	c.mu.Unlock()
	c.changed()
	return ctx, func() {
		c.mu.Lock()
		c.uploadCancel = nil
		c.mu.Unlock()
		cancel()
		c.changed()
	}
}

// completeUTF8 возвращает длину префикса b, не обрывающегося посреди UTF-8 символа.
func completeUTF8(b []byte) int {
	n := len(b)
	for i := 1; i <= 3 && i <= n; i++ {
		ch := b[n-i]
		if ch&0xC0 == 0x80 {
			continue
		}
		if ch&0x80 == 0 {
			return n
		}
		need := 2
		if ch&0xF0 == 0xE0 {
			need = 3
		} else if ch&0xF8 == 0xF0 {
			need = 4
		}
		if i < need {
			return n - i
		}
		return n
	}
	return n
}

func (c *Conn) readLoop(r io.Reader) {
	var strip ansiStripper
	buf := make([]byte, 8192)
	var rest []byte
	var rerr error
	for {
		n, err := r.Read(buf)
		if n > 0 {
			data := append(append([]byte(nil), rest...), buf[:n]...)
			cut := completeUTF8(data)
			rest = data[cut:]
			if text := strip.Strip(string(data[:cut])); text != "" {
				c.feed(text)
			}
		}
		if err != nil {
			rerr = err
			break
		}
	}
	if errors.Is(rerr, io.EOF) {
		rerr = nil
	}
	c.mu.Lock()
	c.flush(c.acc)
	c.acc = ""
	c.mu.Unlock()
	c.closeErr = rerr
	close(c.closed)
	c.out.FlushServer(c.name)
	if !c.closing.Load() {
		c.out.Note(c.name, "соединение закрыто сервером")
	}
	c.changed()
}

// feed разбирает поток: отделяет маркер завершения команды от обычного вывода.
func (c *Conn) feed(text string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.acc += text
	for {
		w := c.waiter
		if w == nil {
			c.flush(c.acc)
			c.acc = ""
			return
		}
		i := strings.Index(c.acc, w.prefix)
		if i < 0 {
			keep := partialSuffix(c.acc, w.prefix)
			c.flush(c.acc[:len(c.acc)-keep])
			c.acc = c.acc[len(c.acc)-keep:]
			return
		}
		rest := c.acc[i+len(w.prefix):]
		j := 0
		for j < len(rest) && rest[j] >= '0' && rest[j] <= '9' {
			j++
		}
		tail := rest[j:]
		if j == len(rest) || (len(tail) < 2 && strings.HasPrefix("__", tail)) {
			// маркер пришёл не целиком — ждём продолжения
			c.flush(c.acc[:i])
			c.acc = c.acc[i:]
			return
		}
		if j == 0 || !strings.HasPrefix(tail, "__") {
			c.flush(c.acc[:i+len(w.prefix)]) // не наш маркер
			c.acc = rest
			continue
		}
		code, _ := strconv.Atoi(rest[:j])
		c.flush(c.acc[:i])
		c.acc = tail[2:]
		c.skipNL = true
		c.waiter = nil
		w.done <- code
	}
}

func partialSuffix(s, prefix string) int {
	for n := len(prefix) - 1; n > 0; n-- {
		if n <= len(s) && strings.HasSuffix(s, prefix[:n]) {
			return n
		}
	}
	return 0
}

// flush отдаёт текст на экран/в лог. Вызывается под c.mu.
func (c *Conn) flush(text string) {
	if c.skipNL {
		t := strings.TrimLeft(text, "\r\n")
		if len(t) > 0 {
			c.skipNL = false
		}
		text = t
	}
	if text == "" {
		return
	}
	c.out.Server(c.name, text)
	if c.sudo && c.cfg.Password != "" {
		c.trackSudo(text)
	}
}

// trackSudo: если оболочка ждёт пароль sudo — отправляем пароль из конфига.
func (c *Conn) trackSudo(text string) {
	for _, r := range text {
		switch r {
		case '\n':
			c.curLine, c.answered = "", false
		case '\r':
			c.curLine = ""
		default:
			c.curLine += string(r)
		}
	}
	if !c.answered && sudoPromptRe.MatchString(c.curLine) {
		c.answered = true
		c.writeStdin(c.cfg.Password + "\n")
		c.out.Note(c.name, "(пароль sudo отправлен автоматически)")
	}
}

func (c *Conn) writeStdin(s string) error {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	_, err := io.WriteString(c.stdin, s)
	return err
}

// Exec выполняет команду в постоянной оболочке и ждёт, пока оболочка снова
// освободится. Возвращает код возврата команды.
func (c *Conn) Exec(cmd string) (int, error) { return c.exec(cmd, 0) }

func (c *Conn) exec(cmd string, timeout time.Duration) (int, error) {
	c.execMu.Lock()
	defer c.execMu.Unlock()
	defer c.changed() // выполняется до Unlock: после завершения команды состояние обновится
	if !c.Alive() {
		return 0, errors.New("нет соединения с сервером")
	}
	id := randHex(6)
	w := &waiter{prefix: "__SR_" + id + "_", done: make(chan int, 1)}
	c.mu.Lock()
	c.waiter = w
	c.busySince, c.busyCmd = time.Now(), oneLine(cmd)
	c.curLine, c.answered = "", false // остатки прошлого вывода (например, приглашения) не мешают
	c.mu.Unlock()
	c.changed()

	// Команда и маркер уходят одним составным оператором «{ cmd \n }; printf ...».
	// Bash разбирает его целиком ДО запуска, поэтому в буфере терминала не остаётся
	// «лишней» строки, которую могла бы прочитать сама команда (sudo, read, ssh ...).
	// id передаётся отдельным аргументом printf, так что эхо этой строки маркером не считается.
	line := "{ " + cmd + "\n}; " + fmt.Sprintf("printf '__SR_%%s_%%s__\\n' '%s' \"$?\"\n", id)
	if err := c.writeStdin(line); err != nil {
		return 0, err
	}
	var tc <-chan time.Time
	if timeout > 0 {
		t := time.NewTimer(timeout)
		defer t.Stop()
		tc = t.C
	}
	select {
	case code := <-w.done:
		c.out.FlushServer(c.name)
		return code, nil
	case <-c.closed:
		c.out.FlushServer(c.name)
		if c.closeErr != nil {
			return 0, fmt.Errorf("соединение потеряно: %v", c.closeErr)
		}
		return 0, errors.New("соединение закрыто (команда завершила сеанс?)")
	case <-tc:
		return 0, errors.New("таймаут ожидания ответа")
	}
}

// SendLine передаёт строку на stdin выполняющейся команды (ответ на вопрос программы и т.п.).
func (c *Conn) SendLine(s string) error { return c.writeStdin(s + "\n") }

// Interrupt посылает Ctrl+C выполняющейся команде.
// Interrupt прерывает то, что сейчас активно на соединении: идёт загрузка файла —
// отменяет её; иначе — Ctrl+C выполняющейся shell-команде.
func (c *Conn) Interrupt() error {
	c.mu.Lock()
	cancel := c.uploadCancel
	c.mu.Unlock()
	if cancel != nil {
		cancel()
		return nil
	}
	return c.writeStdin("\x03")
}

func (c *Conn) Close() {
	c.closing.Store(true)
	c.stdin.Close()
	c.sftpMu.Lock()
	if c.sftp != nil {
		c.sftp.Close()
	}
	c.sftpMu.Unlock()
	c.sess.Close()
	c.cli.Close()
}

// ---------- SFTP и служебные команды ----------

func (c *Conn) sftpc() (*sftp.Client, error) {
	c.sftpMu.Lock()
	defer c.sftpMu.Unlock()
	if c.sftp != nil {
		return c.sftp, nil
	}
	// UseConcurrentWrites — без этого sftp.File.ReadFrom пишет пакеты по одному,
	// ожидая подтверждения каждого (стоп-энд-вейт), из-за чего скорость на канале
	// с заметной задержкой падает до одного пакета за RTT — на дальних серверах
	// это и есть причина «долгой» загрузки больших файлов. С этой опцией пакеты
	// уходят параллельно (конвейером), не дожидаясь ответа по одному.
	sc, err := sftp.NewClient(c.cli,
		sftp.UseConcurrentWrites(true),
		sftp.MaxConcurrentRequestsPerFile(64),
	)
	if err != nil {
		return nil, fmt.Errorf("sftp: %w", err)
	}
	c.sftp = sc
	if h, err := sc.Getwd(); err == nil {
		c.home = h
	}
	return sc, nil
}

// expand раскрывает ведущую ~ (SFTP сам этого не делает).
func (c *Conn) expand(p string) string {
	if p == "~" || strings.HasPrefix(p, "~/") {
		if _, err := c.sftpc(); err == nil && c.home != "" {
			res := path.Join(c.home, p[1:])
			if strings.HasSuffix(p, "/") && !strings.HasSuffix(res, "/") {
				res += "/"
			}
			return res
		}
	}
	return p
}

// runSilent выполняет команду в отдельном канале (не в общей оболочке).
// С sudo=true пароль из конфига подаётся в «sudo -S».
func (c *Conn) runSilent(cmd string, sudo bool) (stdout, stderr string, err error) {
	sess, err := c.cli.NewSession()
	if err != nil {
		return "", "", err
	}
	defer sess.Close()
	var so, se bytes.Buffer
	sess.Stdout, sess.Stderr = &so, &se
	if sudo {
		sess.Stdin = strings.NewReader(c.cfg.Password + "\n")
		cmd = "sudo -S -p '' " + cmd
	}
	err = sess.Run(cmd)
	return so.String(), se.String(), err
}

func isPerm(err error) bool {
	return err != nil && (errors.Is(err, os.ErrPermission) ||
		strings.Contains(strings.ToLower(err.Error()), "permission denied"))
}

// ReadFile читает файл с сервера (при нехватке прав и default_sudo — через sudo cat).
func (c *Conn) ReadFile(p string) ([]byte, error) {
	p = c.expand(p)
	sc, err := c.sftpc()
	if err == nil {
		var f *sftp.File
		if f, err = sc.Open(p); err == nil {
			defer f.Close()
			return io.ReadAll(f)
		}
	}
	if c.sudo && isPerm(err) {
		so, se, e := c.runSilent("cat -- "+shQuote(p), true)
		if e != nil {
			return nil, fmt.Errorf("sudo cat: %v: %s", e, strings.TrimSpace(se))
		}
		return []byte(so), nil
	}
	return nil, err
}

// WriteFile перезаписывает файл на сервере (при нехватке прав и default_sudo — через
// временный файл и «sudo sh -c cat > файл»: владелец и права файла сохраняются).
func (c *Conn) WriteFile(p string, data []byte) error {
	p = c.expand(p)
	sc, err := c.sftpc()
	if err != nil {
		return err
	}
	f, err := sc.OpenFile(p, os.O_WRONLY|os.O_CREATE|os.O_TRUNC)
	if err == nil {
		_, err = f.Write(data)
		if cerr := f.Close(); err == nil {
			err = cerr
		}
		if err == nil {
			return nil
		}
	}
	if !(c.sudo && isPerm(err)) {
		return err
	}
	stage := "/tmp/.sshrun-" + randHex(6)
	if err := sc.Mkdir(stage); err != nil {
		return err
	}
	defer sc.RemoveAll(stage)
	tmp := stage + "/data"
	tf, err := sc.Create(tmp)
	if err != nil {
		return err
	}
	if _, err = tf.Write(data); err != nil {
		tf.Close()
		return err
	}
	if err = tf.Close(); err != nil {
		return err
	}
	_, se, err := c.runSilent("sh -c "+shQuote("cat "+shQuote(tmp)+" > "+shQuote(p)), true)
	if err != nil {
		return fmt.Errorf("sudo запись: %v: %s", err, strings.TrimSpace(se))
	}
	return nil
}
