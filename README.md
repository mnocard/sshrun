[English](README.md) | [Русский](README.ru.md)

# sshrun — run commands on several servers over SSH, step by step

The program connects to several servers over SSH at once and runs a scenario
described in advance in a JSON config (step by step, with pauses and error
handling), and lets you upload files and edit text files on the servers.
Everything that happens is shown on screen at the same time it's written to
a log file.

The program has two modes — graphical (the default) and console (`-cli`).
Both work from the same config and run the scenario the same way; the only
difference is how you see it and what you control it with.

## Build

```
go mod tidy
go build -o sshrun.exe .                             # build for the current OS
GOOS=windows GOARCH=amd64 go build -o sshrun.exe .    # cross-build for Windows
GOOS=darwin  GOARCH=amd64 go build -o sshrun-mac .    # cross-build for macOS
```

A single executable, no install step and no external dependencies — the web
interface is embedded inside it (`go:embed`), and it uses whatever browser is
already installed on the system.

## Running it

```
sshrun.exe                       — graphical mode, pick a config in the window
sshrun.exe -c config.json        — graphical mode, config already known
sshrun.exe -cli -c config.json   — console mode
```

Flags:

| Flag | Meaning |
|---|---|
| `-c path` | config is already known — no picker window/prompt is shown |
| `-cli` | console mode, no browser |
| `-port N` | port for the local web server (default: a random free one) |
| `-no-browser` | don't open a browser window automatically, just print the address to the console |

## Graphical mode

When started without `-c`, the program looks next to itself (in the folder
the .exe is in, and in the current folder) for files named `*config.json`,
case-insensitively — `config.json`, `dev-config.json`, `testconfig.json`,
`123_Config.JSON` and so on all qualify. One found — it opens right away;
several — a list is shown to pick from in the window itself; none — you can
type a path in by hand.

The program starts a web server on `127.0.0.1` (it isn't reachable from
outside your machine) and opens a window with no address bar — Edge or
Chrome in app mode; if neither is present, it opens your regular browser.
The address with the access key is also printed to the console — the same
window can be reopened from it at any time (including if you closed it by
accident); work on the servers isn't interrupted by this: the program runs
the scenario on its own, and the window is just a way to watch it and
control it.

**The program doesn't exit after the last step** — you can repeat any step
(`repeat`), jump to another one (`goto`), or run an arbitrary command; to
quit, use the "Exit" button in the window (or press Ctrl+C twice in the
console the program was started from). After you confirm, it shows a message
that every connection is closed and a 5-second countdown, after which the
window tries to close itself; if the browser won't let a script do that
(there's no guarantee — it depends on the browser), it says so plainly, and
either way the window can always be closed by hand.

In the window: on the left — the list of servers (address,
connected/busy/doing what, a button to interrupt the current command with
Ctrl+C) and the list of steps (status, and for each server — progress and
the error text if there was one); on the right — the live log with an input
line at the bottom. The arrow button next to a server in the list opens the
built-in editor for a file on it. The "⬇ log" button downloads the full log
file for the current session.

If there's more than one server, a row of toggles appears above the log:
"All" and one per server (clicking a server's name in the list on the left
does the same thing). Selecting one shows only its output lines — general
messages such as step headers stay visible under any filter; nothing is
lost — switching back to "All" shows everything that was filtered out.

### About the local web interface's security

Access is protected by a key in the address (visible only in the console,
and after the window is opened, in a browser cookie) and by checking that
the request came from this same machine. It's built on the assumption that
whoever started the program is the one using it, on their own machine — this
is not a public-facing service. **Don't forward the port as-is** (to the
internet or to a shared network without thinking it through) — there's no
encryption, and anyone who gets the address with the key has exactly the
access you do: running commands and reading/writing files on every server in
the config.

If you need access from another computer, the simplest and safest option
already works with no changes at all: an SSH tunnel to the machine the
program is running on —
```
ssh -L 8080:127.0.0.1:PROGRAM_PORT user@machine-running-the-program
```
then open `http://127.0.0.1:8080/...` on your own machine (the program
prints the address with the key to the console on that machine). Full access
over an IP for multiple people is a separate task (TLS, real authentication
instead of a key in the address, permission boundaries) — the current
version doesn't have it.

## Console mode

Started with the `-cli` flag — the whole dialog happens in the terminal, no
browser. Everything below applies to both modes (graphical mode just draws
the same thing in a window instead of as text).

## Config

```json
{
  "servers": [
    {"name": "dev", "host": "10.0.0.5", "port": 22, "username": "user", "password": "***", "color": "#4d8dff"}
  ],
  "settings": {"log_file": "run.log", "connect_timeout": 30, "default_sudo": true, "pause_on_enter": true},
  "highlight": {"rules": [{"pattern": "MY_MARKER", "color": "#ff00ff", "bold": true}]},
  "gui": {"font_size": 14, "max_lines": 20000, "timestamps": true},
  "workflow": [
    {"step_name": "Step", "step_description": "Description", "servers": [
      {"server": "dev", "command": "echo hi"}
    ]}
  ]
}
```

### servers[]

| Field | Meaning |
|---|---|
| `name`, `host`, `port`, `username`, `password` | required connection parameters; `port` defaults to 22 |
| `key_file`, `key_passphrase` | log in with a private key instead of/alongside a password |
| `color` | the server's tag color in graphical mode (a CSS color, e.g. `"#4d8dff"`); if unset, a color is picked automatically from the name |

### workflow[].servers[] (one entry = one or more actions on a server)

| Field | Meaning |
|---|---|
| `command` | a single command |
| `commands` | an array of commands — run in order (can be combined with `command`) |
| `upload` | `{"local": "...", "remote": "..."}` — a file or folder, rules like `scp` |
| `replace` | `{"file": "...", "find": "...", "replace": "...", "regex": false}` — replace text in a file on the server |

Order within one entry: `command`/`commands` → `upload` → `replace`. Several
entries for the same server within a step run on it in order; different
servers within a step run in parallel. An empty `command` just connects to
the server.

### settings

| Field | Meaning |
|---|---|
| `log_file` | defaults to `sshrun.log` |
| `connect_timeout` | seconds, defaults to 30 |
| `default_sudo` | the password from the config is supplied to `sudo` prompts automatically; uploading/editing files without permission goes through sudo (a temp file + `sudo cp` / `sudo sh -c cat`) |
| `pause_on_enter` | defaults to `true` — wait for an empty line after each step; `false` — steps run back to back, stopping only on error |
| `press_any_key_before_next_step` | the same thing (input is line-based, not per-keystroke) |

### highlight — output highlighting

Without this section in the config, built-in rules apply: words like
`error/fail/timeout/denied` — bold red; `warning/deprecated` — yellow;
`success/done/completed` — green; docker statuses like
`unhealthy`/`Exited (1)` — orange.

```json
"highlight": {
  "enabled": true,
  "include_defaults": false,
  "rules": [
    {"pattern": "DEPLOY_OK", "color": "#5fd38d", "bold": true},
    {"pattern": "^\\[FATAL\\]", "regex": true, "background": "#4a1020", "scope": "line", "kinds": ["out"]}
  ]
}
```

| Rule field | Meaning |
|---|---|
| `pattern` | a substring, or a JavaScript regular expression when `regex: true` |
| `ignore_case` | defaults to `true` |
| `color`, `background`, `bold`, `italic`, `underline` | styling; at least one is required |
| `scope` | `"match"` (default — only the matched text is styled) or `"line"` (the whole line) |
| `kinds` | which lines it applies to: `out` (server output), `err`, `warn` (a rejected command/bad input), `sys`, `info`, `cmd`, `in`; defaults to all of them |

`enabled: false` turns highlighting off entirely. Your own `rules` replace
the built-in ones; `include_defaults: true` adds the built-in ones after
yours. Highlighting only applies to graphical mode — it doesn't affect the
log file or console mode.

### gui

| Field | Meaning |
|---|---|
| `font_size` | log font size in px, 9–32, defaults to 13 |
| `max_lines` | how many lines to keep in the window, defaults to 20000 (the log file itself is always complete, never trimmed) |
| `timestamps` | show a timestamp on each line, defaults to `true` |

## Commands in the dialog (after a step, in the console or the window's input line)

| Input | Action |
|---|---|
| Enter (empty line) | run the next step / continue after a pause |
| `<server> <command>` | run a command on a server (`dev,dev-lk <command>` — on several) |
| `all <command>` | on every server |
| `upload <server\|all> <local> <remote>` | upload a file/folder (quote paths that contain spaces) |
| `edit <server> <file>` | open a file in the built-in editor (in CLI mode: download/open in an external editor/send back) |
| `repeat [N]` | repeat step N (defaults to the last one / the one that failed) |
| `retry` | after an error — continue the step from where it stopped |
| `skip` | after an error — ignore it and move on |
| `goto N` | jump to step N and run it |
| `break <server>` / `break server1,server2` / `break all` | Ctrl+C to the command on that server (or those servers); in graphical mode — ⏹ buttons next to "Continue" appear while at least one server is busy |
| `status` / `servers` | list of servers and their addresses |
| `steps`, `help`, `quit` | list of steps, help, exit |

While a command is running, a line of `<server> <text>` is sent to its
stdin (an answer to a `[y/N]` prompt and the like); if exactly one server is
busy, you can type just the answer, without the server name.

In graphical mode, as you type a server name (the first word on the line), a
list of matches pops up under the input field — with each server's address,
plus an `all` entry if there's more than one server. Arrow keys up/down to
pick, Enter or a click to fill it in, Esc to close it.

If the log is filtered to one server (the toggles above the log, see
above), a line with no explicit server name goes to that server — no need to
type `dev ` before every command. Another server's name, and general
commands (`retry`, `goto`, `all` and so on), are left alone by the filter —
the input field's own placeholder text shows where a bare line will go right
now.

## How "command done" and "error" are determined

Each server has a persistent shell open (bash on a PTY) — `cd`, environment
variables and so on persist between commands. After each command, a marker
carrying its exit code is sent down the same stream; until the shell hands
control back, the command is considered still running. The connection is
kept alive actively (keepalives, and dead-connection detection) — this is
meant to hold up for commands that run 20+ minutes.

**An error** is a non-zero exit code, an SFTP/replace/connection failure, or
a lost connection. On error, no new actions start on any server (ones
already running are allowed to finish), and the step is blocked until
`retry` / `repeat` / `skip`.

## File uploads

Done with parallel SFTP packets (rather than one at a time waiting for each
one to be acknowledged) — on a network with noticeable latency this is
tens of times faster. For files 8 MB and up, progress (%, size, speed) is
shown every 2 seconds — the counting itself doesn't slow the transfer down.
Path rules follow `scp`: if the remote path is an existing directory or ends
in `/`, the file/folder is placed inside it under its own name.

While a file or folder is uploading, the server counts as busy (in
graphical mode — the "Stop" button; in either mode — `break <server>`),
exactly as it would during a command. Stopping it is real: the file
currently in flight breaks off right where it was when you clicked, no new
files in the folder start, and the step is marked with the error "upload
cancelled by the user" — you decide what to do next (`retry`/`repeat`/`skip`).

## Log

Everything is written to `log_file` with a timestamp: server output
(`OUT`), commands (`CMD`), user input (`IN`), events (`SYS`), errors
(`ERR`). Passwords never end up in the log. In graphical mode the log can be
downloaded with a button in the window.

## The .exe icon

`rsrc_windows_amd64.syso` sits in the repo root — Go's linker picks up files
like that automatically when building for Windows, no extra CI steps needed
(the only thing that matters is committing it alongside the `.go` files,
same as everything else). It has no effect on Linux/macOS builds — by its
file name it only applies to `windows/amd64`.

The icon's sources are in the `icon/` folder: `icon_master.png` (1024×1024,
what the `.syso` was generated from) and `sshrun.ico` (handy if you want to
set the icon on the file or a shortcut by hand through its properties). To
make your own icon or change the version/description, use
[go-winres](https://github.com/tc-hib/go-winres):

```
go install github.com/tc-hib/go-winres@latest
go-winres simply --arch amd64 --icon icon/icon_master.png \
  --product-name "sshrun" --file-version "0.1.0" --product-version "0.1.0"
```

The command overwrites `rsrc_windows_amd64.syso` — commit the result.

## Limitations

- Don't run `sudo -i`, `su -`, or a bare `bash` as a step — a command like
  that only finishes when you exit the nested shell; use `sudo <command>`
  instead.
- The server's host key isn't verified (`InsecureIgnoreHostKey` in
  `conn.go`).
- Uploading/editing files on a server requires its SFTP subsystem to be
  enabled.
- Passwords are stored in the config in plain text — restrict the file's
  permissions.
- The built-in editor works with UTF-8 text up to 5 MB; for anything bigger,
  use `replace` in the config or an external editor over an SFTP client.
