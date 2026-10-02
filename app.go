package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// ---------- ввод ----------

type inputLine struct {
	s   string
	err error
}

// Input читает stdin строго «по запросу»: строка читается только тогда, когда
// программа готова её принять. Благодаря этому, пока запущен внешний редактор,
// никто не отбирает у него клавиатуру.
type Input struct {
	req     chan struct{}
	ch      chan inputLine
	pending bool
	eof     bool
}

// NewInput — ввод из потока (консольный режим).
func NewInput(r io.Reader) *Input {
	br := bufio.NewReader(r)
	return newInput(func() (string, error) {
		s, err := br.ReadString('\n')
		if err != nil && s == "" {
			return "", err
		}
		return s, nil
	})
}

// NewChanInput — ввод из канала (GUI): каждая строка канала — как введённая строка
// с Enter (пустая строка — просто Enter). Закрытие канала — конец ввода.
func NewChanInput(ch <-chan string) *Input {
	return newInput(func() (string, error) {
		s, ok := <-ch
		if !ok {
			return "", io.EOF
		}
		return s + "\n", nil
	})
}

func newInput(read func() (string, error)) *Input {
	in := &Input{req: make(chan struct{}, 1), ch: make(chan inputLine, 1)}
	go func() {
		for range in.req {
			s, err := read()
			if err != nil {
				in.ch <- inputLine{err: err}
				return
			}
			in.ch <- inputLine{s: s}
		}
	}()
	return in
}

// Wait ждёт строку или закрытия done. Если done == nil — ждёт только строку.
func (in *Input) Wait(done <-chan struct{}) (l inputLine, finished bool) {
	if in.eof {
		if done == nil {
			return inputLine{err: io.EOF}, false
		}
		<-done
		return inputLine{}, true
	}
	if !in.pending {
		in.pending = true
		in.req <- struct{}{}
	}
	select {
	case l = <-in.ch:
		in.pending = false
		if l.err != nil {
			in.eof = true
		}
		return l, false
	case <-done:
		return inputLine{}, true
	}
}

// ---------- состояние ----------

type srvState struct {
	cfg  ServerCfg
	mu   sync.Mutex // держится на время подключения — для быстрых чтений см. cur/connecting
	conn *Conn

	cur        atomic.Pointer[Conn] // то же подключение, но читается без блокировок (для GUI)
	connecting atomic.Bool
}

type progress struct {
	actions []Action
	next    int // сколько действий уже выполнено
	err     error
}

type stepRun struct {
	idx   int
	order []string
	prog  map[string]*progress
}

type App struct {
	cfg *Config
	log *Logger
	out *Console
	in  *Input
	srv map[string]*srvState

	// Поля ниже меняются только под stateMu (читаются также из GUI-горутин).
	stateMu  sync.Mutex
	cur      int      // индекс следующего шага
	last     *stepRun // последний запущенный шаг
	blocked  *stepRun // шаг с неустранёнными ошибками
	mode     string   // running | prompt | blocked | end
	stepStat []string // pending | running | done | failed | skipped
	runs     map[int]*stepRun

	quit         bool
	endAnnounced bool
	closed       atomic.Bool
	shutOnce     sync.Once

	// expectedClose — GUI выставляет перед намеренным закрытием канала ввода
	// (пользователь нажал «Сменить конфиг»), чтобы отличить это от настоящей
	// потери входного потока (например, закрыли консольное окно программы) —
	// раньше оба случая писали в лог одну и ту же фразу, и по логу нельзя
	// было понять, что произошло на самом деле.
	expectedClose atomic.Bool

	// Хуки для GUI (в консольном режиме nil).
	OnState    func()                    // состояние изменилось
	OnExit     func()                    // программа сейчас завершится
	OpenEditor func(server, path string) // открыть встроенный редактор вместо внешнего
}

// ExpectInputClose — вызывается перед намеренным закрытием канала ввода (смена
// конфига в GUI), чтобы promptLoop не принял это за потерю связи.
func (a *App) ExpectInputClose() { a.expectedClose.Store(true) }

func NewApp(cfg *Config, log *Logger, out *Console, in *Input) *App {
	a := &App{cfg: cfg, log: log, out: out, in: in, srv: map[string]*srvState{},
		mode: "running", runs: map[int]*stepRun{}}
	a.stepStat = make([]string, len(cfg.Workflow))
	for i := range a.stepStat {
		a.stepStat[i] = "pending"
	}
	for _, s := range cfg.Servers {
		a.srv[s.Name] = &srvState{cfg: s}
	}
	return a
}

// st выполняет f под stateMu.
func (a *App) st(f func()) {
	a.stateMu.Lock()
	f()
	a.stateMu.Unlock()
}

func (a *App) notify() {
	if a.OnState != nil {
		a.OnState()
	}
}

func (a *App) setMode(m string) {
	a.st(func() { a.mode = m })
	a.notify()
}

// Shutdown закрывает все соединения и лог. Можно вызывать повторно.
func (a *App) Shutdown() {
	a.shutOnce.Do(func() {
		a.closed.Store(true)
		for _, s := range a.srv {
			if c := s.cur.Load(); c != nil { // без s.mu: он может быть занят долгим подключением
				c.Close()
			}
		}
		a.log.Write("SYS", "SYS", "=== сессия завершена ===")
		a.log.Close()
	})
}

// exit завершает программу (с оповещением GUI).
func (a *App) exit(code int) {
	if a.OnExit != nil {
		a.OnExit()
	}
	a.Shutdown()
	os.Exit(code)
}

// conn возвращает живое подключение, при необходимости (пере)подключаясь.
func (a *App) conn(name string) (*Conn, error) {
	s := a.srv[name]
	if s == nil {
		return nil, fmt.Errorf("неизвестный сервер %q", name)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if a.closed.Load() {
		return nil, fmt.Errorf("программа завершает работу")
	}
	if s.conn != nil {
		if s.conn.Alive() {
			return s.conn, nil
		}
		a.out.Note(name, "соединение потеряно, переподключаюсь…")
		s.conn.Close()
		s.conn = nil
		s.cur.Store(nil)
	}
	a.out.Note(name, "подключение к %s@%s:%d …", s.cfg.Username, s.cfg.Host, s.cfg.Port)
	s.connecting.Store(true)
	a.notify()
	c, err := Connect(s.cfg, time.Duration(a.cfg.Settings.ConnectTimeout)*time.Second, a.cfg.Settings.DefaultSudo, a.out, a.notify)
	s.connecting.Store(false)
	if err != nil {
		a.notify()
		return nil, fmt.Errorf("не удалось подключиться: %w", err)
	}
	s.conn = c
	s.cur.Store(c)
	if a.closed.Load() { // Shutdown мог пройти, пока мы подключались
		c.Close()
		return nil, fmt.Errorf("программа завершает работу")
	}
	a.out.Note(name, "подключено")
	a.notify()
	return c, nil
}

func oneLine(s string) string {
	s = strings.ReplaceAll(s, "\n", " ⏎ ")
	if r := []rune(s); len(r) > 120 {
		return string(r[:120]) + "…"
	}
	return s
}

// doAction выполняет одно действие на одном сервере и возвращается, когда оно ЗАВЕРШЕНО
// (для команд — когда оболочка сервера снова готова принимать ввод).
func (a *App) doAction(name string, act Action) error {
	c, err := a.conn(name)
	if err != nil {
		return err
	}
	switch act.Kind {
	case "connect":
		a.out.Note(name, "готов к работе")
	case "cmd":
		a.out.Cmd(name, act.Cmd)
		code, err := c.Exec(act.Cmd)
		if err != nil {
			return err
		}
		if code != 0 {
			return fmt.Errorf("команда завершилась с кодом %d: %s", code, oneLine(act.Cmd))
		}
		a.out.Note(name, "-- готово (код 0)")
	case "upload":
		a.out.Note(name, "загрузка %s -> %s", act.Upload.Local, act.Upload.Remote)
		rep := reporter{
			note:     func(s string) { a.out.Note(name, "%s", s) },
			progress: func(text string, pct int) { a.out.Progress(name, text, pct) },
		}
		if err := c.Upload(act.Upload.Local, act.Upload.Remote, rep); err != nil {
			return fmt.Errorf("загрузка: %w", err)
		}
		a.out.Note(name, "-- загрузка завершена")
	case "replace":
		r := act.Replace
		a.out.Note(name, "замена в %s: %q -> %q", r.File, r.Find, r.Replace)
		n, err := c.Replace(r)
		if err != nil {
			return fmt.Errorf("замена: %w", err)
		}
		a.out.Note(name, "-- заменено вхождений: %d", n)
	default:
		return fmt.Errorf("неизвестное действие %q", act.Kind)
	}
	return nil
}

// ---------- параллельное выполнение ----------

// runParallel запускает fn для каждого сервера одновременно и ждёт всех.
// Пока идёт выполнение, строки пользователя вида «сервер текст» уходят на stdin
// выполняющейся команды на этом сервере, «break сервер» посылает Ctrl+C.
func (a *App) runParallel(names []string, fn func(string) error) map[string]error {
	a.setMode("running")
	errs := map[string]error{}
	var emu sync.Mutex
	var wg sync.WaitGroup
	for _, n := range names {
		wg.Add(1)
		go func(n string) {
			defer wg.Done()
			if err := fn(n); err != nil {
				emu.Lock()
				errs[n] = err
				emu.Unlock()
			}
		}(n)
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	for {
		l, finished := a.in.Wait(done)
		if finished {
			return errs
		}
		if l.err == nil {
			a.handleRunInput(strings.TrimRight(l.s, "\r\n"))
		}
	}
}

func cutWord(s string) (first, rest string) {
	s = strings.TrimSpace(s)
	if i := strings.IndexAny(s, " \t"); i >= 0 {
		return s[:i], strings.TrimSpace(s[i:])
	}
	return s, ""
}

// doBreak шлёт Ctrl+C всем живым подключениям из списка: одно имя сервера,
// «сервер1,сервер2» или all/* — на все сразу. Общая реализация для команды
// break (из строки ввода, в CLI и в GUI) и для кнопок «Остановить» в GUI.
func (a *App) doBreak(tok string) {
	tok = strings.TrimSpace(tok)
	if tok == "" {
		a.out.Warn("Использование: break <сервер>|<сервер1,сервер2>|all")
		return
	}
	targets := a.parseTargets(tok)
	if targets == nil {
		a.out.Warn("Неизвестный сервер %q", tok)
		return
	}
	var hit []string
	for _, n := range targets {
		if c := a.liveConn(n); c != nil {
			c.Interrupt()
			hit = append(hit, n)
		}
	}
	switch len(hit) {
	case 0:
		a.out.Warn("Нет активных подключений среди: %s", strings.Join(targets, ", "))
	case 1:
		a.out.Note(hit[0], "отправлен Ctrl+C")
	default:
		a.out.Info("Ctrl+C отправлен: %s", strings.Join(hit, ", "))
	}
}

func (a *App) handleRunInput(text string) {
	a.out.UserLine(text)
	first, rest := cutWord(text)
	switch strings.ToLower(first) {
	case "quit", "exit":
		a.out.Info("Выход по запросу пользователя")
		a.exit(0)
	case "break":
		a.doBreak(rest)
		return
	}
	// «<сервер> <текст>» — ввод для команды на этом сервере.
	if c := a.liveConn(first); c != nil {
		if c.ExecBusy() {
			c.SendLine(rest)
			a.out.Note(first, "-> ввод передан выполняющейся команде")
		} else if c.Busy() {
			a.out.Note(first, "идёт загрузка файла; прервать — break "+first)
		} else {
			a.out.Note(first, "сейчас ничего не выполняется; новые команды можно вводить после завершения шага")
		}
		return
	}
	// Иначе, если занят ровно один сервер shell-командой, вся строка (в т.ч.
	// пустая) адресуется ему как ввод; загрузка файла тут не в счёт — ей
	// нечего передавать на stdin, оболочка во время неё свободна.
	var busy []string
	for _, s := range a.cfg.Servers {
		if c := a.liveConn(s.Name); c != nil && c.ExecBusy() {
			busy = append(busy, s.Name)
		}
	}
	if len(busy) == 1 {
		a.liveConn(busy[0]).SendLine(text)
		a.out.Note(busy[0], "-> ввод передан выполняющейся команде")
		return
	}
	a.out.Warn("Идёт выполнение. Чтобы передать ввод команде, начните строку с имени сервера: «<сервер> <текст>»; «break <сервер>|all» — Ctrl+C.")
}

func (a *App) liveConn(name string) *Conn {
	s := a.srv[name]
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.conn != nil && s.conn.Alive() {
		return s.conn
	}
	return nil
}

// ---------- шаги ----------

func (a *App) newStepRun(idx int) *stepRun {
	sr := &stepRun{idx: idx, prog: map[string]*progress{}}
	for _, e := range a.cfg.Workflow[idx].Entries {
		p := sr.prog[e.Server]
		if p == nil {
			p = &progress{}
			sr.prog[e.Server] = p
			sr.order = append(sr.order, e.Server)
		}
		p.actions = append(p.actions, e.Actions()...)
	}
	return sr
}

func (a *App) announce(idx int) {
	st := a.cfg.Workflow[idx]
	a.out.Info("")
	a.out.Info("=== Шаг %d/%d: %s ===", idx+1, len(a.cfg.Workflow), st.Name)
	if st.Description != "" {
		a.out.Info("%s", st.Description)
	}
}

// runStep выполняет (или продолжает) шаг: серверы — параллельно, действия
// на каждом сервере — последовательно. При первой ошибке новые действия не
// запускаются нигде; уже идущие команды дожидаются завершения.
func (a *App) runStep(sr *stepRun) bool {
	var targets []string
	for _, n := range sr.order {
		if p := sr.prog[n]; p.next < len(p.actions) {
			targets = append(targets, n)
		}
	}
	a.st(func() {
		a.runs[sr.idx] = sr
		a.stepStat[sr.idx] = "running"
	})
	a.notify()
	var stop atomic.Bool
	a.runParallel(targets, func(n string) error {
		p := sr.prog[n]
		a.st(func() { p.err = nil })
		for p.next < len(p.actions) {
			if stop.Load() {
				return nil
			}
			if err := a.doAction(n, p.actions[p.next]); err != nil {
				a.st(func() { p.err = err })
				stop.Store(true)
				a.out.Err(n, "%v", err)
				a.notify()
				return err
			}
			a.st(func() { p.next++ })
			a.notify()
		}
		return nil
	})

	st := a.cfg.Workflow[sr.idx]
	var failed, waiting []string
	for _, n := range sr.order {
		p := sr.prog[n]
		if p.err != nil {
			failed = append(failed, fmt.Sprintf("%s: %v", n, p.err))
		} else if p.next < len(p.actions) {
			waiting = append(waiting, fmt.Sprintf("%s (осталось действий: %d)", n, len(p.actions)-p.next))
		}
	}
	if len(failed) == 0 && len(waiting) == 0 {
		a.out.Info("Шаг %d «%s» выполнен на всех серверах", sr.idx+1, st.Name)
		a.st(func() { a.stepStat[sr.idx] = "done" })
		a.notify()
		return true
	}
	a.st(func() { a.stepStat[sr.idx] = "failed" })
	a.notify()
	a.out.Info("!!! Шаг %d «%s» ПРИОСТАНОВЛЕН из-за ошибок:", sr.idx+1, st.Name)
	for _, f := range failed {
		a.out.Info("      %s", f)
	}
	if len(waiting) > 0 {
		a.out.Info("    Не выполнено: %s", strings.Join(waiting, ", "))
	}
	a.out.Info("    Устраните проблему (<сервер> <команда>, edit, upload) и введите: retry — продолжить с места ошибки,")
	a.out.Info("    repeat — весь шаг заново, skip — игнорировать ошибку и идти дальше.")
	return false
}

func (a *App) wantPause() bool {
	return *a.cfg.Settings.PauseOnEnter || a.cfg.Settings.PressAnyKey
}

// Run — главный цикл. После последнего шага программа НЕ завершается: она остаётся
// в диалоге (можно повторить шаг, выполнить команды) до quit / закрытия ввода.
func (a *App) Run() {
	for !a.quit {
		if a.cur >= len(a.cfg.Workflow) {
			a.promptLoop() // вернётся только при quit или goto
			continue
		}
		idx := a.cur
		a.st(func() { a.cur++ })
		sr := a.newStepRun(idx)
		a.announce(idx)
		a.finishRun(sr, a.runStep(sr))
		if a.blocked != nil || a.wantPause() || a.cur >= len(a.cfg.Workflow) {
			a.promptLoop()
		}
	}
}

func (a *App) finishRun(sr *stepRun, ok bool) {
	a.st(func() {
		a.last = sr
		if !ok {
			a.blocked = sr
		} else if a.blocked != nil && a.blocked.idx == sr.idx {
			a.blocked = nil
		}
	})
	a.notify()
}

// ---------- диалог с пользователем ----------

func (a *App) promptText() string {
	switch {
	case a.blocked != nil:
		return fmt.Sprintf("\n[ОШИБКА в шаге %d] retry / repeat / skip / <сервер> <команда> > ", a.blocked.idx+1)
	case a.cur >= len(a.cfg.Workflow):
		return "\n[конец] все шаги выполнены; help — справка, quit — выход > "
	default:
		return fmt.Sprintf("\n[след. шаг %d/%d «%s»] Enter — выполнить, help — справка > ",
			a.cur+1, len(a.cfg.Workflow), a.cfg.Workflow[a.cur].Name)
	}
}

// promptLoop ждёт пустую строку (продолжить) или выполняет введённые команды.
// Возвращается, когда можно двигаться дальше (или при quit / goto).
func (a *App) promptLoop() {
	for !a.quit {
		end := a.blocked == nil && a.cur >= len(a.cfg.Workflow)
		if end && !a.endAnnounced {
			a.endAnnounced = true
			a.out.Info("")
			a.out.Info("Все шаги выполнены. Программа продолжает работу: repeat N — повторить шаг, goto N — перейти к шагу,")
			a.out.Info("<сервер> <команда> — выполнить команду, quit — выход.")
		}
		switch {
		case a.blocked != nil:
			a.setMode("blocked")
		case end:
			a.setMode("end")
		default:
			a.setMode("prompt")
		}
		a.out.Prompt(a.promptText())
		l, _ := a.in.Wait(nil)
		if l.err != nil {
			if a.expectedClose.Load() {
				a.out.Info("Сессия закрыта: конфигурация сменена пользователем.")
			} else {
				a.out.Warn("Связь с программой неожиданно потеряна (возможно, закрыто консольное окно, из которого она запущена) — сессия завершена.")
			}
			a.quit = true
			return
		}
		text := strings.TrimSpace(l.s)
		a.out.UserLine(text)
		if text == "" {
			if a.blocked != nil {
				a.out.Warn("Есть неустранённые ошибки — продолжать нельзя. retry / repeat / skip (help — справка).")
				continue
			}
			if a.cur >= len(a.cfg.Workflow) {
				a.out.Warn("Все шаги уже выполнены. Введите команду (help — справка) или quit для выхода.")
				continue
			}
			return
		}
		if a.dispatch(text) {
			return
		}
	}
}

// dispatch выполняет команду пользователя; true — выйти из диалога (goto/quit).
func (a *App) dispatch(text string) bool {
	first, rest := cutWord(text)
	switch strings.ToLower(first) {
	case "help", "?":
		a.help()
	case "steps", "list":
		a.listSteps()
	case "status", "servers", "hosts":
		a.status()
	case "quit", "exit":
		a.quit = true
		return true

	case "goto":
		n, err := strconv.Atoi(rest)
		if err != nil || n < 1 || n > len(a.cfg.Workflow) {
			a.out.Warn("Использование: goto <номер шага 1..%d>", len(a.cfg.Workflow))
			break
		}
		a.st(func() {
			a.cur = n - 1
			a.blocked = nil
		})
		a.endAnnounced = false
		return true

	case "repeat":
		idx := -1
		switch {
		case rest != "":
			n, err := strconv.Atoi(rest)
			if err != nil || n < 1 || n > len(a.cfg.Workflow) {
				a.out.Warn("Использование: repeat [номер шага 1..%d]", len(a.cfg.Workflow))
				return false
			}
			idx = n - 1
		case a.blocked != nil:
			idx = a.blocked.idx
		case a.last != nil:
			idx = a.last.idx
		default:
			a.out.Warn("Ещё ни один шаг не выполнялся")
			return false
		}
		sr := a.newStepRun(idx)
		a.announce(idx)
		a.finishRun(sr, a.runStep(sr))

	case "retry":
		if a.blocked == nil {
			a.out.Warn("Неустранённых ошибок нет. Чтобы выполнить шаг заново: repeat [N]")
			break
		}
		a.out.Info("Продолжаю шаг %d с места остановки…", a.blocked.idx+1)
		a.finishRun(a.blocked, a.runStep(a.blocked))

	case "skip":
		if a.blocked == nil {
			a.out.Warn("Пропускать нечего. Перейти к другому шагу: goto N")
			break
		}
		a.out.Info("Ошибки шага %d проигнорированы по решению пользователя", a.blocked.idx+1)
		a.st(func() {
			a.stepStat[a.blocked.idx] = "skipped"
			a.blocked = nil
		})
		a.notify()

	case "break":
		a.doBreak(rest)

	case "edit":
		args := splitArgs(rest)
		if len(args) != 2 || a.srv[args[0]] == nil {
			a.out.Warn("Использование: edit <сервер> <путь к файлу на сервере>")
			break
		}
		if a.OpenEditor != nil { // GUI: встроенный редактор
			a.OpenEditor(args[0], args[1])
			break
		}
		if err := a.edit(args[0], args[1]); err != nil {
			a.out.Err(args[0], "%v", err)
		}

	case "upload":
		args := splitArgs(rest)
		if len(args) != 3 {
			a.out.Warn("Использование: upload <сервер|сервер1,сервер2|all> <локальный путь> <путь на сервере>")
			break
		}
		targets := a.parseTargets(args[0])
		if targets == nil {
			a.out.Warn("Неизвестный сервер %q", args[0])
			break
		}
		act := Action{Kind: "upload", Upload: UploadCfg{Local: args[1], Remote: args[2]}}
		a.manual(targets, act)

	case "all", "*":
		if rest == "" {
			a.out.Warn("Использование: all <команда>")
			break
		}
		a.manual(a.allServers(), Action{Kind: "cmd", Cmd: rest})

	default:
		if targets := a.parseTargets(first); targets != nil {
			if rest == "" {
				a.out.Warn("После имени сервера нужна команда: %s <команда>", first)
				break
			}
			a.manual(targets, Action{Kind: "cmd", Cmd: rest})
		} else {
			a.out.Warn("Неизвестная команда или сервер %q (help — справка)", first)
		}
	}
	return false
}

func (a *App) allServers() []string {
	var out []string
	for _, s := range a.cfg.Servers {
		out = append(out, s.Name)
	}
	return out
}

func (a *App) parseTargets(tok string) []string {
	if tok == "all" || tok == "*" {
		return a.allServers()
	}
	var out []string
	for _, n := range strings.Split(tok, ",") {
		if a.srv[n] == nil {
			return nil
		}
		out = append(out, n)
	}
	return out
}

// manual выполняет действие, введённое пользователем, на указанных серверах параллельно.
// Ошибки вручную введённых команд не блокируют работу — пользователь и так у руля.
func (a *App) manual(targets []string, act Action) {
	a.runParallel(targets, func(n string) error {
		err := a.doAction(n, act)
		if err != nil {
			a.out.Err(n, "%v", err)
		}
		return err
	})
}

func (a *App) listSteps() {
	for i, st := range a.cfg.Workflow {
		mark := "  "
		if i == a.cur {
			mark = "->"
		}
		a.out.Info("%s %d. %s — %s", mark, i+1, st.Name, st.Description)
	}
}

func (a *App) status() {
	for _, s := range a.cfg.Servers {
		state := "не подключён"
		if a.liveConn(s.Name) != nil {
			state = "подключён"
		}
		a.out.Info("  %-12s %s@%s:%d  %s", s.Name, s.Username, s.Host, s.Port, state)
	}
	if a.blocked != nil {
		a.out.Info("  Шаг %d приостановлен из-за ошибки", a.blocked.idx+1)
	}
}

func (a *App) help() {
	for _, l := range strings.Split(strings.TrimSpace(`
Enter (пустая строка)            выполнить следующий шаг
<сервер> <команда>               выполнить команду на сервере (можно: dev,dev-lk <команда>)
all <команда>                    выполнить команду на всех серверах
upload <сервер|all> <лок> <удал> загрузить файл/каталог (пути с пробелами — в кавычках)
edit <сервер> <файл>             скачать файл, открыть в редакторе, отправить обратно
repeat [N]                       повторить шаг N (по умолчанию — последний/упавший)
retry                            после ошибки: продолжить шаг с места остановки
skip                             после ошибки: игнорировать и идти дальше
goto N                           перейти к шагу N и выполнить его
steps | status | servers         список шагов | серверы: адрес и подключение
break <сервер>|сервер1,сервер2|all  Ctrl+C команде на сервере(ах)
quit                             выход
Пока команда выполняется, строка «<сервер> <текст>» отправляется ей на stdin
(ответ на вопрос [y/N] и т.п.). Редактор: переменная SSHRUN_EDITOR / EDITOR.`), "\n") {
		a.out.Info("  %s", l)
	}
}

// splitArgs делит строку на аргументы с поддержкой "..." и '...'.
// Обратный слэш не экранирует — чтобы не портить пути Windows.
func splitArgs(s string) []string {
	var out []string
	var cur strings.Builder
	var quote rune
	has := false
	for _, r := range s {
		switch {
		case quote != 0:
			if r == quote {
				quote = 0
			} else {
				cur.WriteRune(r)
			}
		case r == '"' || r == '\'':
			quote, has = r, true
		case r == ' ' || r == '\t':
			if has || cur.Len() > 0 {
				out = append(out, cur.String())
				cur.Reset()
				has = false
			}
		default:
			cur.WriteRune(r)
		}
	}
	if has || cur.Len() > 0 {
		out = append(out, cur.String())
	}
	return out
}

// ---------- редактирование файла ----------

func editorCommand(file string) ([]string, error) {
	spec := ""
	for _, k := range []string{"SSHRUN_EDITOR", "VISUAL", "EDITOR"} {
		if v := os.Getenv(k); v != "" {
			spec = v
			break
		}
	}
	if spec == "" {
		if runtime.GOOS == "windows" {
			spec = "notepad"
		} else {
			for _, e := range []string{"nano", "vim", "vi"} {
				if _, err := exec.LookPath(e); err == nil {
					spec = e
					break
				}
			}
		}
	}
	if spec == "" {
		return nil, fmt.Errorf("не найден редактор: задайте переменную SSHRUN_EDITOR")
	}
	return append(splitArgs(spec), file), nil
}

// edit: скачивает файл, открывает в локальном редакторе и отправляет обратно, если он изменился.
func (a *App) edit(name, remote string) error {
	c, err := a.conn(name)
	if err != nil {
		return err
	}
	data, err := c.ReadFile(remote)
	isNew := false
	if err != nil {
		if !strings.Contains(strings.ToLower(err.Error()), "not exist") &&
			!strings.Contains(strings.ToLower(err.Error()), "no such file") {
			return fmt.Errorf("чтение %s: %w", remote, err)
		}
		isNew, data = true, nil
		a.out.Note(name, "файла %s нет — будет создан новый", remote)
	}
	if bytes.IndexByte(data, 0) >= 0 {
		return fmt.Errorf("%s похож на бинарный файл — редактирование отменено", remote)
	}

	dir, err := os.MkdirTemp("", "sshrun-edit-")
	if err != nil {
		return err
	}
	keep := false // при неудачной отправке оставляем правки на диске
	defer func() {
		if !keep {
			os.RemoveAll(dir)
		}
	}()
	local := filepath.Join(dir, path.Base(remote))
	if err := os.WriteFile(local, data, 0o600); err != nil {
		return err
	}
	args, err := editorCommand(local)
	if err != nil {
		return err
	}
	a.out.Note(name, "%s -> %s (%s); открываю редактор: %s", remote, local, humanSize(int64(len(data))), args[0])
	cmd := exec.Command(args[0], args[1:]...)
	cmd.Stdin, cmd.Stdout, cmd.Stderr = os.Stdin, os.Stdout, os.Stderr
	if err := cmd.Run(); err != nil {
		a.out.Note(name, "редактор завершился с ошибкой: %v (продолжаем)", err)
	}

	// Некоторые редакторы (новый Блокнот Windows, VS Code без -w) возвращают управление сразу.
	a.out.Prompt("Сохраните файл и нажмите Enter, чтобы отправить его на сервер (n — отмена) > ")
	l, _ := a.in.Wait(nil)
	ans := strings.ToLower(strings.TrimSpace(l.s))
	a.out.UserLine(ans)
	if l.err != nil || ans == "n" || ans == "no" || ans == "н" || ans == "нет" {
		a.out.Note(name, "отправка отменена, файл на сервере не изменён")
		return nil
	}
	newData, err := os.ReadFile(local)
	if err != nil {
		return err
	}
	if !isNew && bytes.Equal(newData, data) {
		a.out.Note(name, "файл не изменился, отправлять нечего")
		return nil
	}
	if err := c.WriteFile(remote, newData); err != nil {
		keep = true
		return fmt.Errorf("отправка %s: %w (правки сохранены локально: %s)", remote, err, local)
	}
	a.out.Note(name, "%s обновлён на сервере (%s -> %s)", remote, humanSize(int64(len(data))), humanSize(int64(len(newData))))
	return nil
}
