package main

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// findConfigs ищет в dir файлы, чьё имя (без учёта регистра) заканчивается на
// «config.json» — под это подходит и обычный config.json, и dev-config.json,
// testconfig.json, 123_config.json и т.п.
func findConfigs(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if strings.HasSuffix(strings.ToLower(e.Name()), "config.json") {
			out = append(out, e.Name())
		}
	}
	sort.Strings(out)
	return out, nil
}

// pickConfig просит пользователя выбрать файл из списка — номером или именем.
func pickConfig(dir string, names []string) (string, error) {
	fmt.Println("Рядом с программой найдено несколько файлов конфигурации:")
	for i, n := range names {
		fmt.Printf("  %d. %s\n", i+1, n)
	}
	br := bufio.NewReader(os.Stdin)
	for {
		fmt.Print("Выберите номер (или введите имя файла) > ")
		line, err := br.ReadString('\n')
		if err != nil {
			return "", fmt.Errorf("ввод прерван, конфиг не выбран")
		}
		line = strings.TrimSpace(line)
		if n, cerr := strconv.Atoi(line); cerr == nil && n >= 1 && n <= len(names) {
			return filepath.Join(dir, names[n-1]), nil
		}
		for _, n := range names {
			if strings.EqualFold(n, line) {
				return filepath.Join(dir, n), nil
			}
		}
		fmt.Println("Не понял выбор — введите номер из списка выше или точное имя файла.")
	}
}

// resolveConfigPath: явно заданный путь (-c или позиционный аргумент) имеет
// приоритет и используется без вопросов — это нужно для автоматизации и скриптов.
// Иначе ищем *config.json рядом с исполняемым файлом программы: один найденный
// файл выбирается автоматически, несколько — предлагаются на выбор, ни одного —
// используется прежнее поведение по умолчанию, config.json в текущем каталоге.
func resolveConfigPath(explicitPath string, explicit bool) (string, error) {
	if explicit {
		return explicitPath, nil
	}
	dir := "."
	if exe, err := os.Executable(); err == nil {
		if real, err := filepath.EvalSymlinks(exe); err == nil {
			exe = real
		}
		dir = filepath.Dir(exe)
	}
	names, err := findConfigs(dir)
	if err != nil || len(names) == 0 {
		return "config.json", nil
	}
	if len(names) == 1 {
		p := filepath.Join(dir, names[0])
		fmt.Println("Конфиг:", p)
		return p, nil
	}
	return pickConfig(dir, names)
}
