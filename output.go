package main

import (
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"
)

// ---------- лог ----------

// Logger пишет всё в файл: вывод серверов (OUT), отправленные команды (CMD),
// ввод пользователя (IN), служебные сообщения (SYS) и ошибки (ERR).
type Logger struct {
	mu sync.Mutex
	f  *os.File
}

func NewLogger(path string) (*Logger, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	return &Logger{f: f}, nil
}

func (l *Logger) Write(who, kind, text string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	ts := time.Now().Format("2006-01-02 15:04:05.000")
	text = strings.ReplaceAll(text, "\r", "")
	for _, line := range strings.Split(text, "\n") {
		fmt.Fprintf(l.f, "%s [%s] %-3s %s\n", ts, who, kind, line)
	}
}

func (l *Logger) Close() { l.f.Close() }

// ---------- консоль ----------

type lineState struct {
	buf     []rune
	cr      bool // на конце потока был \r
	midline bool // на экране осталась недописанная строка этого сервера
	hidden  bool // не показывать на экране (только в лог)
	timer   *time.Timer
}

// Console печатает вывод нескольких серверов вперемешку, с префиксом [сервер],
// и одновременно пишет всё в лог.
type Console struct {
	mu    sync.Mutex
	w     io.Writer
	log   *Logger
	width int
	st    map[string]*lineState
	open  string // кто сейчас держит незавершённую строку на экране
}

const promptOwner = "\x00prompt"

func NewConsole(w io.Writer, log *Logger, names []string) *Console {
	c := &Console{w: w, log: log, st: map[string]*lineState{}}
	for _, n := range names {
		if l := len([]rune(n)); l > c.width {
			c.width = l
		}
	}
	return c
}

func (c *Console) state(name string) *lineState {
	s := c.st[name]
	if s == nil {
		s = &lineState{}
		c.st[name] = s
	}
	return s
}

func (c *Console) prefix(name string) string {
	pad := c.width - len([]rune(name))
	if pad < 0 {
		pad = 0
	}
	return "[" + name + strings.Repeat(" ", pad) + "] "
}

// breakLine закрывает чужую незавершённую строку переводом строки.
func (c *Console) breakLine(except string) {
	if c.open != "" && c.open != except {
		fmt.Fprint(c.w, "\n")
		if s := c.st[c.open]; s != nil {
			s.midline = false
		}
		c.open = ""
	}
}

func (c *Console) Hide(name string, hidden bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.state(name).hidden = hidden
}

// Server принимает «сырой» (уже без ANSI) вывод сервера.
func (c *Console) Server(name, text string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	st := c.state(name)
	for _, r := range text {
		if st.cr {
			st.cr = false
			if r != '\n' { // одиночный \r — перезапись строки (прогресс-бары)
				st.buf = st.buf[:0]
			}
		}
		switch r {
		case '\r':
			st.cr = true
		case '\n':
			c.emit(name, string(st.buf), true)
			st.buf = st.buf[:0]
		default:
			st.buf = append(st.buf, r)
		}
	}
	if len(st.buf) > 0 { // приглашения вида «Password: » без \n — выводим по таймеру
		if st.timer == nil {
			st.timer = time.AfterFunc(250*time.Millisecond, func() { c.FlushServer(name) })
		} else {
			st.timer.Reset(250 * time.Millisecond)
		}
	}
}

func (c *Console) FlushServer(name string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	st := c.state(name)
	if len(st.buf) > 0 {
		c.emit(name, string(st.buf), false)
		st.buf = st.buf[:0]
	}
}

func (c *Console) emit(name, text string, complete bool) {
	st := c.state(name)
	c.log.Write(name, "OUT", text)
	if st.hidden {
		st.midline = false
		return
	}
	c.breakLine(name)
	if !st.midline {
		fmt.Fprint(c.w, c.prefix(name))
	}
	fmt.Fprint(c.w, text)
	if complete {
		fmt.Fprint(c.w, "\n")
		st.midline = false
		if c.open == name {
			c.open = ""
		}
	} else {
		st.midline = true
		c.open = name
	}
}

func (c *Console) line(who, kind, text string, prefixed bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.breakLine("")
	if prefixed {
		fmt.Fprint(c.w, c.prefix(who))
	}
	fmt.Fprintln(c.w, text)
	c.log.Write(who, kind, text)
}

// Cmd — команда, отправляемая на сервер.
func (c *Console) Cmd(name, cmd string) { c.line(name, "CMD", "$ "+cmd, true) }

// Note — служебное сообщение о конкретном сервере.
func (c *Console) Note(name, format string, a ...any) {
	c.line(name, "SYS", fmt.Sprintf(format, a...), true)
}

// Err — ошибка на конкретном сервере.
func (c *Console) Err(name, format string, a ...any) {
	c.line(name, "ERR", "!!! ОШИБКА: "+fmt.Sprintf(format, a...), true)
}

// Info — общее сообщение программы.
func (c *Console) Info(format string, a ...any) {
	c.line("SYS", "SYS", fmt.Sprintf(format, a...), false)
}

func (c *Console) Prompt(text string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.breakLine("")
	fmt.Fprint(c.w, text)
	c.open = promptOwner
}

// UserLine вызывается после того, как пользователь нажал Enter.
func (c *Console) UserLine(text string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.open = ""
	c.log.Write("user", "IN", text)
}

// ---------- ANSI ----------

// ansiStripper вырезает управляющие последовательности терминала.
// Хранит состояние между вызовами (последовательность может разорваться между чтениями).
type ansiStripper struct{ st int }

func (a *ansiStripper) Strip(s string) string {
	var sb strings.Builder
	for _, r := range s {
		switch a.st {
		case 0:
			switch {
			case r == 0x1b:
				a.st = 1
			case r == '\n' || r == '\r' || r == '\t':
				sb.WriteRune(r)
			case r < 0x20 || r == 0x7f:
				// прочие управляющие символы (BEL, BS...) отбрасываем
			default:
				sb.WriteRune(r)
			}
		case 1: // после ESC
			switch r {
			case '[':
				a.st = 2
			case ']', 'P', 'X', '^', '_':
				a.st = 3
			case '(', ')', '*', '+':
				a.st = 4
			default:
				a.st = 0
			}
		case 2: // CSI ... финальный байт 0x40–0x7E
			if r >= 0x40 && r <= 0x7e {
				a.st = 0
			}
		case 3: // OSC/DCS до BEL или ESC \
			if r == 0x07 {
				a.st = 0
			} else if r == 0x1b {
				a.st = 4 // следующий символ ('\') съедим
			}
		case 4:
			a.st = 0
		}
	}
	return sb.String()
}
