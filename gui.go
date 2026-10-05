package main

import (
	"bufio"
	"crypto/subtle"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Веб-интерфейс встроен в исполняемый файл: программа поднимает локальный HTTP-сервер
// на 127.0.0.1 и открывает окно браузера (Edge/Chrome в режиме «приложения», без адресной
// строки). Никаких внешних зависимостей и файлов рядом с .exe не нужно.
//
//go:embed web/index.html
var indexHTML []byte

// ---------- брокер событий (SSE) ----------

type sseMsg struct {
	id    int64  // 0 — без id
	event string // "" — обычное сообщение (строка вывода)
	data  []byte
}

type subscriber struct{ ch chan sseMsg }

// Broker хранит последние события вывода (чтобы после перезагрузки страницы или обрыва
// связи окно восстановило картину) и раздаёт новые всем подключённым окнам.
type Broker struct {
	mu       sync.Mutex
	seq      int64
	resetSeq int64
	buf      []sseMsg
	subs     map[*subscriber]struct{}
	state    []byte
}

const brokerMaxBuf = 100000

func NewBroker() *Broker { return &Broker{subs: map[*subscriber]struct{}{}} }

// fan вызывается под b.mu.
func (b *Broker) fan(m sseMsg) {
	for s := range b.subs {
		select {
		case s.ch <- m:
		default:
			// Окно не успевает читать — отключаем; EventSource переподключится
			// и получит пропущенное по Last-Event-ID.
			close(s.ch)
			delete(b.subs, s)
		}
	}
}

func (b *Broker) PublishEvent(ev Event) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.seq++
	ev.Seq = b.seq
	data, _ := json.Marshal(ev)
	m := sseMsg{id: ev.Seq, data: data}
	b.buf = append(b.buf, m)
	if len(b.buf) > brokerMaxBuf {
		b.buf = append([]sseMsg(nil), b.buf[len(b.buf)-brokerMaxBuf*9/10:]...)
	}
	b.fan(m)
}

func (b *Broker) PublishNamed(event string, data []byte) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.fan(sseMsg{event: event, data: data})
}

func (b *Broker) SetState(data []byte) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.state = data
	b.fan(sseMsg{event: "state", data: data})
}

// Reset начинает новую сессию: старый вывод забывается, окна очищаются.
func (b *Broker) Reset() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.buf = nil
	b.resetSeq = b.seq
	b.fan(sseMsg{event: "reset", data: []byte("{}")})
}

func (b *Broker) Subscribe(since int64) (sub *subscriber, replay []sseMsg, state []byte, gap bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	sub = &subscriber{ch: make(chan sseMsg, 8192)}
	if since > 0 && since <= b.resetSeq {
		gap = true // окно помнит вывод предыдущей сессии
	}
	if !gap && since > 0 && len(b.buf) > 0 && b.buf[0].id > since+1 {
		gap = true // часть событий уже вытеснена из буфера
	}
	if gap {
		since = 0
	}
	i := sort.Search(len(b.buf), func(i int) bool { return b.buf[i].id > since })
	replay = append(replay, b.buf[i:]...)
	b.subs[sub] = struct{}{}
	return sub, replay, b.state, gap
}

func (b *Broker) Unsubscribe(s *subscriber) {
	b.mu.Lock()
	delete(b.subs, s)
	b.mu.Unlock()
}

// ---------- конфиги и состояние ----------

type ConfigRef struct {
	Name string `json:"name"`
	Path string `json:"path"`
	Dir  string `json:"dir"`
}

func findConfigRefs(dirs ...string) []ConfigRef {
	seen := map[string]bool{}
	var out []ConfigRef
	for _, d := range dirs {
		abs, err := filepath.Abs(d)
		if err != nil || seen[abs] {
			continue
		}
		seen[abs] = true
		names, err := findConfigs(abs)
		if err != nil {
			continue
		}
		for _, n := range names {
			out = append(out, ConfigRef{Name: n, Path: filepath.Join(abs, n), Dir: abs})
		}
	}
	return out
}

type UIHighlight struct {
	Enabled bool            `json:"enabled"`
	Rules   []HighlightRule `json:"rules"`
}

// UIConfig — настройки внешнего вида для текущей сессии (из конфига).
type UIConfig struct {
	FontSize   int         `json:"font_size"`
	MaxLines   int         `json:"max_lines"`
	Timestamps bool        `json:"timestamps"`
	Highlight  UIHighlight `json:"highlight"`
}

type guiState struct {
	Loaded  bool        `json:"loaded"`
	Configs []ConfigRef `json:"configs"`
	Error   string      `json:"error,omitempty"`
	Session int         `json:"session"`
	Name    string      `json:"name,omitempty"`
	Log     string      `json:"log,omitempty"`
	App     *AppSnap    `json:"app,omitempty"`
	UI      *UIConfig   `json:"ui,omitempty"`
}

// Session — одна загруженная конфигурация и работающее по ней приложение.
type Session struct {
	path      string
	cfg       *Config
	app       *App
	ui        *UIConfig
	lines     chan string
	mu        sync.Mutex
	closed    bool
	switching bool // сессия закрывается ради выбора другого конфига, а не ради выхода
}

var errSessionClosed = errors.New("сессия закрыта")

func (s *Session) send(text string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errSessionClosed
	}
	select {
	case s.lines <- text:
		return nil
	default:
		return errors.New("очередь ввода переполнена")
	}
}

func (s *Session) closeInput() {
	s.mu.Lock()
	if !s.closed {
		s.closed = true
		close(s.lines)
	}
	s.mu.Unlock()
}

type GUI struct {
	token  string
	addr   string
	broker *Broker
	dirs   []string

	mu      sync.Mutex // sess, configs, lastErr, sessID
	sess    *Session
	configs []ConfigRef
	lastErr string
	sessID  int

	pushMu sync.Mutex // порядок публикации снимков состояния
}

func (g *GUI) session() *Session {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.sess
}

func (g *GUI) refreshConfigs() {
	refs := findConfigRefs(g.dirs...)
	g.mu.Lock()
	g.configs = refs
	g.mu.Unlock()
}

func (g *GUI) snapshot() guiState {
	g.mu.Lock()
	st := guiState{Configs: g.configs, Error: g.lastErr, Session: g.sessID}
	s := g.sess
	g.mu.Unlock()
	if st.Configs == nil {
		st.Configs = []ConfigRef{}
	}
	if s != nil {
		st.Loaded = true
		st.Name = filepath.Base(s.path)
		st.Log = s.cfg.Settings.LogFile
		snap := s.app.snapshot()
		st.App = &snap
		st.UI = s.ui
	}
	return st
}

// pushState публикует актуальный снимок состояния всем окнам.
func (g *GUI) pushState() {
	g.pushMu.Lock()
	defer g.pushMu.Unlock()
	data, err := json.Marshal(g.snapshot())
	if err == nil {
		g.broker.SetState(data)
	}
}

func (g *GUI) setErr(msg string) {
	g.mu.Lock()
	g.lastErr = msg
	g.mu.Unlock()
}

// load загружает конфиг и запускает выполнение шагов.
func (g *GUI) load(path string) error {
	if g.session() != nil {
		return errors.New("конфиг уже загружен — сначала «Сменить конфиг»")
	}
	cfg, err := LoadConfig(path)
	if err != nil {
		g.setErr(err.Error())
		g.pushState()
		return err
	}
	cfg.Settings.LogFile = ComputeLogFileName(cfg) // своё имя лога, значение из конфига игнорируется
	logger, err := NewLogger(cfg.Settings.LogFile)
	if err != nil {
		err = fmt.Errorf("не удалось открыть лог %s: %w", cfg.Settings.LogFile, err)
		g.setErr(err.Error())
		g.pushState()
		return err
	}
	names := make([]string, 0, len(cfg.Servers))
	for _, s := range cfg.Servers {
		names = append(names, s.Name)
	}
	g.broker.Reset()
	lines := make(chan string, 256)
	// os.Stdout — вывод одновременно идёт и в это окно браузера (через SetSink
	// ниже), и в то консольное окно, из которого запущена программа.
	out := NewConsole(os.Stdout, logger, names)
	out.SetSink(g.broker.PublishEvent)
	app := NewApp(cfg, logger, out, NewChanInput(lines))

	enabled, rules := cfg.HighlightEffective()
	ui := &UIConfig{FontSize: cfg.GUI.FontSize, MaxLines: cfg.GUI.MaxLines, Timestamps: *cfg.GUI.Timestamps}
	ui.Highlight = UIHighlight{Enabled: enabled, Rules: rules}
	if ui.Highlight.Rules == nil {
		ui.Highlight.Rules = []HighlightRule{}
	}

	s := &Session{path: path, cfg: cfg, app: app, ui: ui, lines: lines}
	app.OnState = g.pushState
	app.OnExit = g.beforeExit
	app.OpenEditor = func(server, p string) {
		data, _ := json.Marshal(map[string]string{"action": "edit", "server": server, "path": p})
		g.broker.PublishNamed("ui", data)
	}

	g.mu.Lock()
	if g.sess != nil {
		g.mu.Unlock()
		logger.Close()
		return errors.New("конфиг уже загружен")
	}
	g.sess = s
	g.sessID++
	g.lastErr = ""
	g.mu.Unlock()

	logger.Write("SYS", "SYS", fmt.Sprintf("=== сессия начата, конфиг: %s, лог: %s ===", path, cfg.Settings.LogFile))
	out.Info("Конфиг: %s, лог: %s. Справка — help.", path, cfg.Settings.LogFile)
	g.pushState()
	go g.run(s)
	return nil
}

func (g *GUI) run(s *Session) {
	s.app.Run()
	s.app.Shutdown()
	g.mu.Lock()
	sw := s.switching
	g.sess = nil
	g.mu.Unlock()
	if !sw {
		g.exit()
		return
	}
	g.refreshConfigs()
	g.pushState()
}

// unload закрывает текущую сессию (соединения рвутся) и возвращает к выбору конфига.
func (g *GUI) unload() error {
	g.mu.Lock()
	s := g.sess
	if s != nil {
		s.switching = true
	}
	g.mu.Unlock()
	if s == nil {
		return errors.New("конфиг не загружен")
	}
	s.app.ExpectInputClose()
	s.closeInput()
	s.app.Shutdown() // не ждём окончания долгих команд
	return nil
}

func (g *GUI) beforeExit() {
	g.broker.PublishNamed("bye", []byte("{}"))
	time.Sleep(300 * time.Millisecond) // дать окну получить сообщение
}

func (g *GUI) exit() {
	g.beforeExit()
	if s := g.session(); s != nil {
		s.app.Shutdown()
	}
	os.Exit(0)
}

// ---------- HTTP ----------

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

func fail(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}

func readJSON(w http.ResponseWriter, r *http.Request, v any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, 16<<20)
	if err := json.NewDecoder(r.Body).Decode(v); err != nil {
		fail(w, http.StatusBadRequest, "некорректный запрос: "+err.Error())
		return false
	}
	return true
}

func post(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			fail(w, http.StatusMethodNotAllowed, "нужен POST")
			return
		}
		h(w, r)
	}
}

// guard: через этот сервер можно выполнять команды на серверах, поэтому доступ строго
// локальный и по секрету: (1) только запросы с Host 127.0.0.1/localhost — защита от
// DNS-rebinding; (2) токен из адреса запуска превращается в cookie SameSite=Strict;
// (3) у POST-запросов Origin, если он есть, должен совпадать с адресом сервера.
func (g *GUI) guard(next http.Handler) http.Handler {
	_, port, _ := net.SplitHostPort(g.addr)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Страница управляет реальными серверами — не даём встраивать её в <iframe>
		// на постороннем сайте (clickjacking): проверка Host выше защищает от
		// DNS-rebinding, но не от показа этой же страницы поверх чужой в iframe.
		w.Header().Set("X-Frame-Options", "DENY")
		w.Header().Set("Content-Security-Policy", "frame-ancestors 'none'")
		if r.Host != g.addr && r.Host != "localhost:"+port {
			http.Error(w, "forbidden host", http.StatusForbidden)
			return
		}
		ok := false
		if t := r.URL.Query().Get("t"); t != "" && subtle.ConstantTimeCompare([]byte(t), []byte(g.token)) == 1 {
			http.SetCookie(w, &http.Cookie{Name: "sshrun_auth", Value: g.token, Path: "/", HttpOnly: true, SameSite: http.SameSiteStrictMode})
			if r.URL.Path == "/" { // убираем токен из адресной строки
				http.Redirect(w, r, "/", http.StatusFound)
				return
			}
			ok = true
		} else if c, err := r.Cookie("sshrun_auth"); err == nil && subtle.ConstantTimeCompare([]byte(c.Value), []byte(g.token)) == 1 {
			ok = true
		}
		if !ok {
			http.Error(w, "Откройте адрес, который выведла программа при запуске (в нём есть ключ доступа).", http.StatusUnauthorized)
			return
		}
		if r.Method == http.MethodPost {
			if o := r.Header.Get("Origin"); o != "" && o != "http://"+r.Host {
				http.Error(w, "forbidden origin", http.StatusForbidden)
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}

func (g *GUI) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("Cache-Control", "no-store")
		w.Write(indexHTML)
	})
	mux.HandleFunc("/api/events", g.handleEvents)
	mux.HandleFunc("/api/state", func(w http.ResponseWriter, r *http.Request) { writeJSON(w, 200, g.snapshot()) })
	mux.HandleFunc("/api/load", post(g.handleLoad))
	mux.HandleFunc("/api/refresh", post(func(w http.ResponseWriter, r *http.Request) {
		g.refreshConfigs()
		g.pushState()
		writeJSON(w, 200, map[string]bool{"ok": true})
	}))
	mux.HandleFunc("/api/unload", post(func(w http.ResponseWriter, r *http.Request) {
		if err := g.unload(); err != nil {
			fail(w, http.StatusConflict, err.Error())
			return
		}
		writeJSON(w, 200, map[string]bool{"ok": true})
	}))
	mux.HandleFunc("/api/cmd", post(g.handleCmd))
	mux.HandleFunc("/api/quit", post(func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, 200, map[string]bool{"ok": true})
		go g.exit()
	}))
	mux.HandleFunc("/api/file/read", post(g.handleFileRead))
	mux.HandleFunc("/api/file/write", post(g.handleFileWrite))
	mux.HandleFunc("/api/log", g.handleLog)
	return g.guard(mux)
}

func (g *GUI) handleLoad(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Path string `json:"path"`
	}
	if !readJSON(w, r, &req) {
		return
	}
	req.Path = strings.TrimSpace(strings.Trim(strings.TrimSpace(req.Path), `"`))
	if req.Path == "" {
		fail(w, http.StatusBadRequest, "не указан путь к конфигу")
		return
	}
	if err := g.load(req.Path); err != nil {
		fail(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, 200, map[string]bool{"ok": true})
}

func (g *GUI) handleCmd(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Text string `json:"text"`
	}
	if !readJSON(w, r, &req) {
		return
	}
	if strings.ContainsAny(req.Text, "\r\n") {
		fail(w, http.StatusBadRequest, "команда должна быть одной строкой")
		return
	}
	s := g.session()
	if s == nil {
		fail(w, http.StatusConflict, "конфиг не загружен")
		return
	}
	if err := s.send(req.Text); err != nil {
		fail(w, http.StatusConflict, err.Error())
		return
	}
	writeJSON(w, 200, map[string]bool{"ok": true})
}

func (g *GUI) handleFileRead(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Server string `json:"server"`
		Path   string `json:"path"`
	}
	if !readJSON(w, r, &req) {
		return
	}
	s := g.session()
	if s == nil {
		fail(w, http.StatusConflict, "конфиг не загружен")
		return
	}
	if strings.TrimSpace(req.Path) == "" {
		fail(w, http.StatusBadRequest, "не указан путь к файлу")
		return
	}
	f, err := s.app.ReadRemote(req.Server, req.Path)
	if err != nil {
		fail(w, http.StatusBadGateway, err.Error())
		return
	}
	writeJSON(w, 200, f)
}

func (g *GUI) handleFileWrite(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Server  string `json:"server"`
		Path    string `json:"path"`
		Content string `json:"content"`
		CRLF    bool   `json:"crlf"`
	}
	if !readJSON(w, r, &req) {
		return
	}
	s := g.session()
	if s == nil {
		fail(w, http.StatusConflict, "конфиг не загружен")
		return
	}
	if err := s.app.WriteRemote(req.Server, req.Path, req.Content, req.CRLF); err != nil {
		fail(w, http.StatusBadGateway, err.Error())
		return
	}
	writeJSON(w, 200, map[string]bool{"ok": true})
}

func (g *GUI) handleLog(w http.ResponseWriter, r *http.Request) {
	s := g.session()
	if s == nil {
		http.NotFound(w, r)
		return
	}
	p := s.cfg.Settings.LogFile
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("Content-Disposition", `attachment; filename="`+filepath.Base(p)+`"`)
	http.ServeFile(w, r, p)
}

func (g *GUI) handleEvents(w http.ResponseWriter, r *http.Request) {
	fl, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	h := w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-cache")
	h.Set("X-Accel-Buffering", "no")
	var since int64
	if v := r.Header.Get("Last-Event-ID"); v != "" {
		since, _ = strconv.ParseInt(v, 10, 64)
	} else if v := r.URL.Query().Get("since"); v != "" {
		since, _ = strconv.ParseInt(v, 10, 64)
	}
	sub, replay, state, gap := g.broker.Subscribe(since)
	defer g.broker.Unsubscribe(sub)

	bw := bufio.NewWriterSize(w, 64<<10)
	write := func(m sseMsg) {
		if m.event != "" {
			fmt.Fprintf(bw, "event: %s\n", m.event)
		}
		if m.id != 0 {
			fmt.Fprintf(bw, "id: %d\n", m.id)
		}
		fmt.Fprintf(bw, "data: %s\n\n", m.data)
	}
	// Сначала состояние (в нём настройки подсветки), потом накопленный вывод.
	if state != nil {
		write(sseMsg{event: "state", data: state})
	}
	if gap {
		write(sseMsg{event: "reset", data: []byte("{}")})
	}
	for _, m := range replay {
		write(m)
	}
	bw.Flush()
	fl.Flush()

	ping := time.NewTicker(15 * time.Second)
	defer ping.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case m, ok := <-sub.ch:
			if !ok {
				return
			}
			write(m)
		drain:
			for {
				select {
				case m2, ok2 := <-sub.ch:
					if !ok2 {
						bw.Flush()
						fl.Flush()
						return
					}
					write(m2)
				default:
					break drain
				}
			}
			bw.Flush()
			fl.Flush()
		case <-ping.C:
			io.WriteString(bw, ": ping\n\n")
			bw.Flush()
			fl.Flush()
		}
	}
}

// ---------- запуск ----------

// openWindow открывает интерфейс в отдельном окне без адресной строки (Edge/Chrome
// в режиме --app), а если их нет — в браузере по умолчанию.
func openWindow(url string) {
	start := func(name string, args ...string) bool {
		return exec.Command(name, args...).Start() == nil
	}
	app := []string{"--app=" + url, "--window-size=1400,900"}
	switch runtime.GOOS {
	case "windows":
		pf, pf86, local := os.Getenv("ProgramFiles"), os.Getenv("ProgramFiles(x86)"), os.Getenv("LocalAppData")
		for _, p := range []string{
			filepath.Join(pf86, `Microsoft\Edge\Application\msedge.exe`),
			filepath.Join(pf, `Microsoft\Edge\Application\msedge.exe`),
			filepath.Join(pf, `Google\Chrome\Application\chrome.exe`),
			filepath.Join(pf86, `Google\Chrome\Application\chrome.exe`),
			filepath.Join(local, `Google\Chrome\Application\chrome.exe`),
		} {
			if _, err := os.Stat(p); err == nil && start(p, app...) {
				return
			}
		}
		start("rundll32", "url.dll,FileProtocolHandler", url)
	case "darwin":
		for _, b := range []string{"Google Chrome", "Microsoft Edge"} {
			if _, err := os.Stat("/Applications/" + b + ".app"); err == nil {
				if start("open", append([]string{"-na", b, "--args"}, app...)...) {
					return
				}
			}
		}
		start("open", url)
	default:
		for _, b := range []string{"google-chrome", "chromium", "chromium-browser", "microsoft-edge"} {
			if p, err := exec.LookPath(b); err == nil && start(p, app...) {
				return
			}
		}
		start("xdg-open", url)
	}
}

func runGUI(path string, explicit bool, port int, noBrowser bool) {
	exeDir := "."
	if exe, err := os.Executable(); err == nil {
		if real, err := filepath.EvalSymlinks(exe); err == nil {
			exe = real
		}
		exeDir = filepath.Dir(exe)
	}
	cwd, _ := os.Getwd()

	ln, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		fmt.Fprintln(os.Stderr, "Не удалось запустить веб-интерфейс:", err)
		os.Exit(2)
	}
	g := &GUI{token: randHex(16), broker: NewBroker(), dirs: []string{exeDir, cwd}, addr: ln.Addr().String()}
	g.refreshConfigs()
	srv := &http.Server{Handler: g.routes(), ReadHeaderTimeout: 10 * time.Second}
	go srv.Serve(ln)

	switch {
	case explicit:
		if err := g.load(path); err != nil {
			fmt.Fprintln(os.Stderr, "Ошибка конфигурации:", err)
		}
	case len(g.configs) == 1:
		g.load(g.configs[0].Path)
	default:
		g.pushState()
	}

	url := fmt.Sprintf("http://%s/?t=%s", g.addr, g.token)
	fmt.Println("sshrun GUI:", url)
	fmt.Println("Окно можно закрыть и открыть снова по этому адресу — работа на серверах не прерывается.")
	fmt.Println("Завершить программу: кнопка «Выход» в окне или двойное Ctrl+C здесь.")
	if !noBrowser {
		openWindow(url)
	}

	sig := make(chan os.Signal, 4)
	signal.Notify(sig, os.Interrupt)
	var last time.Time
	for range sig {
		if !last.IsZero() && time.Since(last) < 3*time.Second {
			g.exit()
		}
		last = time.Now()
		fmt.Println("Ctrl+C: нажмите ещё раз в течение 3 с, чтобы завершить программу (соединения с серверами будут закрыты).")
	}
}
