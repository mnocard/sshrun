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

type Config struct {
	Servers  []ServerCfg `json:"servers"`
	Settings Settings    `json:"settings"`
	Workflow []Step      `json:"workflow"`
}

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
	"edit": true, "upload": true, "all": true, "*": true,
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
