package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"time"
)

func main() {
	cfgPath := flag.String("c", "config.json", "путь к файлу конфигурации (если не задан — выбор среди *config.json рядом с программой)")
	cli := flag.Bool("cli", false, "консольный режим без графического интерфейса")
	port := flag.Int("port", 0, "порт локального веб-интерфейса (по умолчанию — случайный свободный)")
	noBrowser := flag.Bool("no-browser", false, "не открывать окно автоматически, только вывести адрес")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Использование: sshrun [-cli] [-c config.json] [-port N] [-no-browser]\n")
	}
	flag.Parse()

	explicit := false
	flag.Visit(func(f *flag.Flag) {
		if f.Name == "c" {
			explicit = true
		}
	})
	path := *cfgPath
	if flag.NArg() > 0 {
		path = flag.Arg(0)
		explicit = true
	}

	if *cli {
		runCLI(path, explicit)
		return
	}
	runGUI(path, explicit, *port, *noBrowser)
}

// runCLI — консольный режим (как в первых версиях программы).
func runCLI(path string, explicit bool) {
	path, err := resolveConfigPath(path, explicit)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Ошибка выбора конфига:", err)
		os.Exit(2)
	}

	cfg, err := LoadConfig(path)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Ошибка конфигурации:", err)
		os.Exit(2)
	}
	cfg.Settings.LogFile = ComputeLogFileName(cfg) // своё имя лога, значение из конфига игнорируется
	logger, err := NewLogger(cfg.Settings.LogFile)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Не удалось открыть лог:", err)
		os.Exit(2)
	}
	names := make([]string, 0, len(cfg.Servers))
	for _, s := range cfg.Servers {
		names = append(names, s.Name)
	}
	out := NewConsole(os.Stdout, logger, names)
	app := NewApp(cfg, logger, out, NewInput(os.Stdin))

	// Ctrl+C не убивает программу сразу (иначе легко оборвать работу на серверах):
	// повторное нажатие в течение 3 секунд — выход.
	sig := make(chan os.Signal, 4)
	signal.Notify(sig, os.Interrupt)
	go func() {
		var last time.Time
		for range sig {
			if !last.IsZero() && time.Since(last) < 3*time.Second {
				out.Info("Принудительный выход")
				app.exit(130)
			}
			last = time.Now()
			out.Info("Ctrl+C: нажмите ещё раз в течение 3 с для выхода. Прервать команду на сервере: break <сервер>")
		}
	}()

	logger.Write("SYS", "SYS", fmt.Sprintf("=== сессия начата, конфиг: %s, лог: %s ===", path, cfg.Settings.LogFile))
	out.Info("Конфиг: %s, лог: %s. Справка — help.", path, cfg.Settings.LogFile)
	app.Run()
	app.Shutdown()
}
