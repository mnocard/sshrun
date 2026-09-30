package main

import (
	"bytes"
	"fmt"
	"strings"
	"unicode/utf8"
)

// ---------- снимок состояния для GUI ----------

type StepServerSnap struct {
	Server string `json:"server"`
	Done   int    `json:"done"`
	Total  int    `json:"total"`
	Err    string `json:"err,omitempty"`
}

type StepSnap struct {
	Name        string           `json:"name"`
	Description string           `json:"description"`
	Status      string           `json:"status"` // pending | running | done | failed | skipped
	Servers     []StepServerSnap `json:"servers,omitempty"`
}

type ServerSnap struct {
	Name      string `json:"name"`
	Host      string `json:"host"`
	Port      int    `json:"port"`
	User      string `json:"user"`
	Color     string `json:"color,omitempty"`
	State     string `json:"state"` // disconnected | connecting | idle | busy
	BusySince int64  `json:"busy_since,omitempty"`
	Cmd       string `json:"cmd,omitempty"`
}

type AppSnap struct {
	Mode    string       `json:"mode"`    // running | prompt | blocked | end
	Cur     int          `json:"cur"`     // индекс следующего шага
	Blocked int          `json:"blocked"` // индекс шага с ошибкой или -1
	Steps   []StepSnap   `json:"steps"`
	Servers []ServerSnap `json:"servers"`
}

// snapshot безопасно вызывается из любой горутины и не блокируется на подключении.
func (a *App) snapshot() AppSnap {
	a.stateMu.Lock()
	snap := AppSnap{Mode: a.mode, Cur: a.cur, Blocked: -1}
	if a.blocked != nil {
		snap.Blocked = a.blocked.idx
	}
	for i, st := range a.cfg.Workflow {
		ss := StepSnap{Name: st.Name, Description: st.Description, Status: a.stepStat[i]}
		if sr := a.runs[i]; sr != nil {
			for _, n := range sr.order {
				p := sr.prog[n]
				e := StepServerSnap{Server: n, Done: p.next, Total: len(p.actions)}
				if p.err != nil {
					e.Err = p.err.Error()
				}
				ss.Servers = append(ss.Servers, e)
			}
		}
		snap.Steps = append(snap.Steps, ss)
	}
	a.stateMu.Unlock()

	for _, sc := range a.cfg.Servers {
		st := a.srv[sc.Name]
		s := ServerSnap{Name: sc.Name, Host: sc.Host, Port: sc.Port, User: sc.Username, Color: sc.Color, State: "disconnected"}
		if st.connecting.Load() {
			s.State = "connecting"
		} else if c := st.cur.Load(); c != nil && c.Alive() {
			s.State = "idle"
			if busy, since, cmd := c.BusyInfo(); busy {
				s.State = "busy"
				s.BusySince = since.UnixMilli()
				s.Cmd = cmd
			}
		}
		snap.Servers = append(snap.Servers, s)
	}
	return snap
}

// ---------- удалённые файлы для встроенного редактора ----------

const maxEditSize = 5 << 20

type RemoteFile struct {
	Content string `json:"content"`
	CRLF    bool   `json:"crlf"`   // в файле были переводы строк Windows — при сохранении вернём их
	IsNew   bool   `json:"is_new"` // файла нет — будет создан
	Size    int    `json:"size"`
}

// ReadRemote читает текстовый файл с сервера для редактирования в браузере.
func (a *App) ReadRemote(server, remote string) (*RemoteFile, error) {
	if a.srv[server] == nil {
		return nil, fmt.Errorf("неизвестный сервер %q", server)
	}
	c, err := a.conn(server)
	if err != nil {
		return nil, err
	}
	data, err := c.ReadFile(remote)
	if err != nil {
		low := strings.ToLower(err.Error())
		if strings.Contains(low, "not exist") || strings.Contains(low, "no such file") {
			a.out.Note(server, "edit: файла %s нет — будет создан новый", remote)
			return &RemoteFile{IsNew: true}, nil
		}
		return nil, fmt.Errorf("чтение %s: %w", remote, err)
	}
	if len(data) > maxEditSize {
		return nil, fmt.Errorf("%s: файл %s больше %s — для такого размера используйте replace или внешний редактор",
			remote, humanSize(int64(len(data))), humanSize(maxEditSize))
	}
	if bytes.IndexByte(data, 0) >= 0 {
		return nil, fmt.Errorf("%s похож на бинарный файл — редактирование отменено", remote)
	}
	if !utf8.Valid(data) {
		// Иначе браузер подменит непонятные байты, и сохранение повредит файл.
		return nil, fmt.Errorf("%s не в кодировке UTF-8 — редактирование в окне программы отключено, чтобы не испортить файл", remote)
	}
	crlf := bytes.Contains(data, []byte("\r\n"))
	text := string(data)
	if crlf {
		text = strings.ReplaceAll(text, "\r\n", "\n")
	}
	a.out.Note(server, "edit: открыт %s (%s)", remote, humanSize(int64(len(data))))
	return &RemoteFile{Content: text, CRLF: crlf, Size: len(data)}, nil
}

// WriteRemote отправляет отредактированный файл обратно на сервер.
func (a *App) WriteRemote(server, remote, content string, crlf bool) error {
	if a.srv[server] == nil {
		return fmt.Errorf("неизвестный сервер %q", server)
	}
	c, err := a.conn(server)
	if err != nil {
		return err
	}
	if crlf {
		content = strings.ReplaceAll(strings.ReplaceAll(content, "\r\n", "\n"), "\n", "\r\n")
	}
	if err := c.WriteFile(remote, []byte(content)); err != nil {
		a.out.Err(server, "edit: не удалось сохранить %s: %v", remote, err)
		return fmt.Errorf("отправка %s: %w", remote, err)
	}
	a.out.Note(server, "edit: %s сохранён на сервере (%s, строк: %d)",
		remote, humanSize(int64(len(content))), strings.Count(content, "\n")+1)
	return nil
}
