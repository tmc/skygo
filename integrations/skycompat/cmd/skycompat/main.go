package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	skyv1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/integrations/skycompat/tasklower"
	"github.com/tmc/skygo/integrations/skycompat/taskyaml"
)

const usage = "usage: skycompat import --shell PATH [flags] TASK.yaml|-\n"

func main() {
	os.Exit(run(os.Args[1:], os.Stdin, os.Stdout, os.Stderr))
}

// run executes one command and returns its exit status.
func run(args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprint(stderr, usage)
		return 2
	}
	switch args[0] {
	case "import":
		return importTask(args[1:], stdin, stdout, stderr)
	case "help", "-h", "-help", "--help":
		fmt.Fprint(stdout, usage)
		return 0
	}
	fmt.Fprintf(stderr, "skycompat: unknown command %q\n%s", args[0], usage)
	return 2
}

func importTask(args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("import", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() {
		fmt.Fprint(stderr, usage, "\nflags:\n")
		fs.PrintDefaults()
	}
	var opts tasklower.Options
	var ref, timeout string
	once(fs, &opts.Shell, "shell", "absolute `path` of the shell that runs run and setup text as PATH -c TEXT")
	once(fs, &ref, "workdir-ref", "artifact reference `file` written by skygo artifacts upload; required with workdir")
	once(fs, &opts.Target, "target", "skygo target `name`")
	once(fs, &timeout, "timeout-seconds", "attempt timeout in `seconds`, 0..604800; 0 keeps the server default")
	fs.Func("output", "result `root` relative to the workdir; repeatable", func(s string) error {
		opts.Outputs = append(opts.Outputs, s)
		return nil
	})
	fs.Func("allow-drop", "drop the nonexecuting `field` name or _user_specified_yaml; repeatable", func(s string) error {
		opts.AllowDrop = append(opts.AllowDrop, s)
		return nil
	})
	var envs []string
	fs.Func("env", "override environment `KEY=VALUE`; repeatable", func(s string) error {
		envs = append(envs, s)
		return nil
	})
	if err := fs.Parse(args); err != nil {
		if err == flag.ErrHelp {
			return 0
		}
		return 2
	}
	if fs.NArg() != 1 {
		fs.Usage()
		return 2
	}
	// Environment values may be secrets, so these errors never echo them.
	env := map[string]string{}
	for _, s := range envs {
		k, v, ok := strings.Cut(s, "=")
		if !ok || k == "" {
			fmt.Fprintln(stderr, "skycompat: --env requires KEY=VALUE")
			return 2
		}
		if _, dup := env[k]; dup {
			fmt.Fprintln(stderr, "skycompat: duplicate --env key")
			return 2
		}
		env[k] = v
	}
	if timeout != "" {
		n, err := strconv.ParseInt(timeout, 10, 64)
		if err != nil {
			fmt.Fprintln(stderr, "skycompat: invalid --timeout-seconds")
			return 2
		}
		opts.TimeoutSeconds = n
	}
	task, notes, err := lower(fs.Arg(0), ref, env, opts, stdin)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	if _, err := stdout.Write(task); err != nil {
		fmt.Fprintf(stderr, "skycompat: write task: %v\n", err)
		return 1
	}
	var b bytes.Buffer
	for _, n := range notes {
		fmt.Fprintf(&b, "%s\t%s\t%s\n", n.Path, n.Status, n.Detail)
	}
	if _, err := stderr.Write(b.Bytes()); err != nil {
		return 1
	}
	return 0
}

// lower reads the explicit inputs and returns the encoded task and notes.
func lower(file, ref string, env map[string]string, opts tasklower.Options, stdin io.Reader) ([]byte, []tasklower.Note, error) {
	if ref != "" {
		w, err := readRef(ref)
		if err != nil {
			return nil, nil, err
		}
		opts.Workdir = w
	}
	r := stdin
	if file != "-" {
		f, err := os.Open(file)
		if err != nil {
			return nil, nil, fmt.Errorf("skycompat: %w", err)
		}
		defer f.Close()
		r = f
	}
	d, err := taskyaml.Parse(r, taskyaml.Options{EnvOverrides: env})
	if err != nil {
		return nil, nil, err
	}
	task, notes, err := tasklower.Lower(d, opts)
	if err != nil {
		return nil, nil, err
	}
	b, err := tasklower.Marshal(task)
	if err != nil {
		return nil, nil, err
	}
	return b, notes, nil
}

// readRef decodes the artifact reference JSON object skygo writes. Like
// skygo, it refuses unknown, duplicate and noncanonical fields, and a size
// that is not an unquoted integer.
func readRef(file string) (*skyv1.ArtifactRef, error) {
	f, err := os.Open(file)
	if err != nil {
		return nil, fmt.Errorf("skycompat: %w", err)
	}
	defer f.Close()
	b, err := io.ReadAll(io.LimitReader(f, 4097))
	if err != nil {
		return nil, fmt.Errorf("skycompat: read workdir reference: %w", err)
	}
	if len(b) > 4096 {
		return nil, errors.New("skycompat: workdir reference exceeds 4 KiB")
	}
	invalid := errors.New("skycompat: invalid workdir reference")
	d := json.NewDecoder(bytes.NewReader(b))
	d.UseNumber()
	if t, err := d.Token(); err != nil || t != json.Delim('{') {
		return nil, invalid
	}
	ref := &skyv1.ArtifactRef{}
	seen := map[string]bool{}
	for d.More() {
		t, err := d.Token()
		name, ok := t.(string)
		if err != nil || !ok || seen[name] {
			return nil, invalid
		}
		seen[name] = true
		switch name {
		case "id":
			err = d.Decode(&ref.Id)
		case "sha256":
			err = d.Decode(&ref.Sha256)
		case "size":
			// Decoding into a json.Number also accepts a quoted number;
			// the token's type tells them apart.
			t, err = d.Token()
			n, number := t.(json.Number)
			if err == nil && !number {
				err = invalid
			}
			if err == nil {
				ref.Size, err = n.Int64()
			}
			if err == nil && strconv.FormatInt(ref.Size, 10) != string(n) {
				err = invalid
			}
		default:
			return nil, invalid
		}
		if err != nil {
			return nil, invalid
		}
	}
	if _, err := d.Token(); err != nil {
		return nil, invalid
	}
	if _, err := d.Token(); err != io.EOF {
		return nil, invalid
	}
	if len(seen) != 3 {
		return nil, invalid
	}
	return ref, nil
}

// once defines a string flag that refuses a second value.
func once(fs *flag.FlagSet, p *string, name, usage string) {
	set := false
	fs.Func(name, usage, func(s string) error {
		if set {
			return errors.New("repeated")
		}
		set = true
		*p = s
		return nil
	})
}
