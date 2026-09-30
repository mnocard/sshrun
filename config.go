package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

type ServerCfg struct {
	Name     string `json:"name"`
	Host     string `json:"host"`
	Port     int    `json:"port"`
	Username string `json:"username"`
	Password string `json:"password"`
	KeyFile  string `json:"key_file"`
	KeyPass  string `json:"key_passphrase"`
	Color    string `json:"color"` // цвет метки сервера в GUI (CSS-цвет); по умолчанию — автоматически
}

type Settings struct {
	LogFile        string `json:"log_file"`
	ConnectTimeout int    `json:"connect_timeout"`
	DefaultSudo    bool   `json:"default_sudo"`
	PressAnyKey    bool   `json:"press_any_key_before_next_step"`
	PauseOnEnter   *bool  `json:"pause_on_enter"`
}

type UploadCfg struct {
	Local  string `json:"local"`
	Remote string `json:"remote"`
}

type ReplaceCfg struct {
	File    string `json:"file"`
	Find    string `json:"find"`
	Replace string `json:"replace"`
	Regex   bool   `json:"regex"`
}

// Entry — то, что нужно сделать на одном сервере в рамках шага.
// Порядок внутри записи: command(s) -> upload -> replace.
// Несколько записей с одним и тем же сервером выполняются последовательно.
type Entry struct {
	Server   string      `json:"server"`
	Command  string      `json:"command"`
	Commands []string    `json:"commands"`
	Upload   *UploadCfg  `json:"upload"`
	Replace  *ReplaceCfg `json:"replace"`
}

type Step struct {
	Name        string  `json:"step_name"`
	Description string  `json:"step_description"`
	Entries     []Entry `json:"servers"`
}

// HighlightRule — правило подсветки вывода в GUI.
// Pattern — подстрока (или регулярное выражение JavaScript при regex=true).
// Scope: "match" (по умолчанию) — красится только найденный текст,
// "line" — красится вся строка. Kinds ограничивает типы строк
// (out, err, sys, info, cmd, in); по умолчанию — out, err, sys, info.
type HighlightRule struct {
	Pattern    string   `json:"pattern"`
	Regex      bool     `json:"regex,omitempty"`
	IgnoreCase *bool    `json:"ignore_case,omitempty"` // по умолчанию true
	Color      string   `json:"color,omitempty"`
	Background string   `json:"background,omitempty"`
	Bold       bool     `json:"bold,omitempty"`
	Italic     bool     `json:"italic,omitempty"`
	Underline  bool     `json:"underline,omitempty"`
	Scope      string   `json:"scope,omitempty"`
	Kinds      []string `json:"kinds,omitempty"`
}

type HighlightCfg struct {
	Enabled         *bool           `json:"enabled"`          // false — подсветка выключена совсем
	IncludeDefaults bool            `json:"include_defaults"` // добавить встроенные правила после своих
	Rules           []HighlightRule `json:"rules"`
}

type GUICfg struct {
	FontSize   int   `json:"font_size"`  // размер шрифта вывода, px (по умолчанию 13)
	MaxLines   int   `json:"max_lines"`  // сколько строк держать в окне (по умолчанию 20000; лог-файл полный)
	Timestamps *bool `json:"timestamps"` // показывать время у строк (по умолчанию true)
}

type Config struct {
	Servers   []ServerCfg   `json:"servers"`
	Settings  Settings      `json:"settings"`
	Workflow  []Step        `json:"workflow"`
	Highlight *HighlightCfg `json:"highlight"`
	GUI       GUICfg        `json:"gui"`
}

// defaultHighlightRules — встроенные правила, если в конфиге нет своих.
func defaultHighlightRules() []HighlightRule {
	return []HighlightRule{
		{Pattern: `\b(error|errors|fail|failed|failure|fails|fatal|panic|exception|traceback|denied|refused|unreachable|critical)\b|no such file|not found|cannot |can't |unable to|timed? ?out|ошибк[а-яё]*|не удалось|отказано|не найден[а-яё]*`, Regex: true, Color: "#ff6b6b", Bold: true},
		{Pattern: `\b(warn|warning|warnings|deprecated)\b|предупрежден[а-яё]*`, Regex: true, Color: "#f0c05a", Bold: true},
		{Pattern: `\b(success|successful|successfully|done|passed|healthy|finished|completed)\b|успешно|готово|выполнено`, Regex: true, Color: "#5fd38d"},
		{Pattern: `unhealthy|Exited \(\d+\)|Restarting|\bDead\b`, Regex: true, Color: "#ff9f43"},
	}
}

// HighlightEffective возвращает итоговый набор правил: свои правила заменяют встроенные
// (если не задан include_defaults); без раздела highlight действуют встроенные.
func (c *Config) HighlightEffective() (bool, []HighlightRule) {
	h := c.Highlight
	if h == nil {
		return true, defaultHighlightRules()
	}
	if h.Enabled != nil && !*h.Enabled {
		return false, nil
	}
	if len(h.Rules) == 0 {
		return true, defaultHighlightRules()
	}
	rules := append([]HighlightRule(nil), h.Rules...)
	if h.IncludeDefaults {
		rules = append(rules, defaultHighlightRules()...)
	}
	return true, rules
}

var highlightKinds = map[string]bool{"out": true, "err": true, "sys": true, "info": true, "cmd": true, "in": true}

// Action — одно атомарное действие на сервере.
type Action struct {
	Kind    string // connect | cmd | upload | replace
	Cmd     string
	Upload  UploadCfg
	Replace ReplaceCfg
}

func (e Entry) Actions() []Action {
	var out []Action
	if strings.TrimSpace(e.Command) != "" {
		out = append(out, Action{Kind: "cmd", Cmd: e.Command})
	}
	for _, c := range e.Commands {
		if strings.TrimSpace(c) != "" {
			out = append(out, Action{Kind: "cmd", Cmd: c})
		}
	}
	if e.Upload != nil {
		out = append(out, Action{Kind: "upload", Upload: *e.Upload})
	}
	if e.Replace != nil {
		out = append(out, Action{Kind: "replace", Replace: *e.Replace})
	}
	if len(out) == 0 {
		out = append(out, Action{Kind: "connect"})
	}
	return out
}

// Слова, которые нельзя использовать как имена серверов (они — команды программы).
var reserved = map[string]bool{
	"help": true, "steps": true, "list": true, "status": true, "quit": true, "exit": true,
	"repeat": true, "goto": true, "retry": true, "skip": true, "break": true,
	"edit": true, "upload": true, "all": true, "*": true, "servers": true, "hosts": true,
}

func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	data = bytes.TrimPrefix(data, []byte("\xef\xbb\xbf")) // BOM от Windows-редакторов
	var cfg Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("ошибка разбора %s: %w", path, err)
	}

	s := &cfg.Settings
	if s.LogFile == "" {
		s.LogFile = "sshrun.log"
	}
	if s.ConnectTimeout <= 0 {
		s.ConnectTimeout = 30
	}
	if s.PauseOnEnter == nil {
		t := true
		s.PauseOnEnter = &t
	}

	g := &cfg.GUI
	if g.FontSize == 0 {
		g.FontSize = 13
	}
	if g.FontSize < 9 || g.FontSize > 32 {
		return nil, fmt.Errorf("gui.font_size должен быть от 9 до 32")
	}
	if g.MaxLines == 0 {
		g.MaxLines = 20000
	}
	if g.MaxLines < 500 {
		g.MaxLines = 500
	}
	if g.Timestamps == nil {
		t := true
		g.Timestamps = &t
	}
	if cfg.Highlight != nil {
		for i, r := range cfg.Highlight.Rules {
			where := fmt.Sprintf("highlight.rules[%d]", i)
			if r.Pattern == "" {
				return nil, fmt.Errorf("%s: не задан pattern", where)
			}
			if r.Scope != "" && r.Scope != "match" && r.Scope != "line" {
				return nil, fmt.Errorf("%s: scope должен быть \"match\" или \"line\"", where)
			}
			for _, k := range r.Kinds {
				if !highlightKinds[k] {
					return nil, fmt.Errorf("%s: неизвестный тип строки %q (допустимо: out, err, sys, info, cmd, in)", where, k)
				}
			}
			if r.Color == "" && r.Background == "" && !r.Bold && !r.Italic && !r.Underline {
				return nil, fmt.Errorf("%s: правило ничего не меняет — задайте color, background, bold, italic или underline", where)
			}
		}
	}

	if len(cfg.Servers) == 0 {
		return nil, fmt.Errorf("в конфиге нет ни одного сервера")
	}
	known := map[string]bool{}
	for i := range cfg.Servers {
		sv := &cfg.Servers[i]
		if sv.Port == 0 {
			sv.Port = 22
		}
		switch {
		case sv.Name == "":
			return nil, fmt.Errorf("servers[%d]: не задано name", i)
		case strings.ContainsAny(sv.Name, " \t,"):
			return nil, fmt.Errorf("сервер %q: в имени не должно быть пробелов и запятых", sv.Name)
		case reserved[strings.ToLower(sv.Name)]:
			return nil, fmt.Errorf("сервер %q: имя совпадает с командой программы", sv.Name)
		case known[sv.Name]:
			return nil, fmt.Errorf("сервер %q описан дважды", sv.Name)
		case sv.Host == "" || sv.Username == "":
			return nil, fmt.Errorf("сервер %q: нужны host и username", sv.Name)
		}
		known[sv.Name] = true
	}

	if len(cfg.Workflow) == 0 {
		return nil, fmt.Errorf("в конфиге нет шагов (workflow)")
	}
	for i, st := range cfg.Workflow {
		for j, e := range st.Entries {
			where := fmt.Sprintf("шаг %d (%s), запись %d", i+1, st.Name, j+1)
			if !known[e.Server] {
				return nil, fmt.Errorf("%s: неизвестный сервер %q", where, e.Server)
			}
			if e.Upload != nil && (e.Upload.Local == "" || e.Upload.Remote == "") {
				return nil, fmt.Errorf("%s: у upload должны быть local и remote", where)
			}
			if e.Replace != nil && (e.Replace.File == "" || e.Replace.Find == "") {
				return nil, fmt.Errorf("%s: у replace должны быть file и find", where)
			}
		}
	}
	return &cfg, nil
}
