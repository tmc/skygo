package ec2ctl

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const goodConfig = `{
	"version": 1,
	"profile": {
		"account": "123456789012",
		"region": "us-west-2",
		"subnet_id": "subnet-0123456789abcdef0",
		"security_group_ids": ["sg-0bbbbbbbbbbbbbbbb", "sg-0aaaaaaaaaaaaaaaa"],
		"image_id": "ami-0123456789abcdef0",
		"instance_type": "t3.micro",
		"root_device": "/dev/xvda",
		"root_volume_gib": 16,
		"root_volume_type": "gp3",
		"encrypt_root": true
	},
	"identity": {"owner": "install-7f3a", "environment": "env-0123456789abcdef", "generation": 1},
	"user_data": "#cloud-config\n"
}`

func TestParseConfig(t *testing.T) {
	cfg, err := ParseConfig(strings.NewReader(goodConfig))
	if err != nil {
		t.Fatal(err)
	}
	want := testConfig()
	if cfg.Profile.Hash() != want.Profile.Hash() || cfg.Identity != want.Identity || string(cfg.UserData) != string(want.UserData) {
		t.Fatalf("parsed %+v, want %+v", cfg, want)
	}

	bad := []struct{ name, text string }{
		{"unknown field", strings.Replace(goodConfig, `"version": 1,`, `"version": 1, "endpoint": "https://example.com",`, 1)},
		{"trailing data", goodConfig + "{}"},
		{"version 2", strings.Replace(goodConfig, `"version": 1`, `"version": 2`, 1)},
		{"no version", strings.Replace(goodConfig, `"version": 1,`, ``, 1)},
		{"invalid profile", strings.Replace(goodConfig, `"t3.micro"`, `"t3 micro"`, 1)},
		{"no generation", strings.Replace(goodConfig, `"generation": 1`, `"generation": 0`, 1)},
		{"user data too large", strings.Replace(goodConfig, `"#cloud-config\n"`, `"`+strings.Repeat("x", 16<<10+1)+`"`, 1)},
		{"too large", goodConfig + strings.Repeat(" ", maxConfig)},
	}
	for _, tt := range bad {
		if _, err := ParseConfig(strings.NewReader(tt.text)); err == nil {
			t.Errorf("%s: accepted", tt.name)
		}
	}
}

func TestLoadConfig(t *testing.T) {
	dir := t.TempDir()
	write := func(name, text string, mode os.FileMode) string {
		p := filepath.Join(dir, name)
		must(t, os.WriteFile(p, []byte(text), mode))
		must(t, os.Chmod(p, mode))
		return p
	}
	good := write("good.json", goodConfig, 0o600)
	cfg, err := LoadConfig(good)
	if err != nil {
		t.Fatal(err)
	}
	if want := testConfig(); cfg.Profile.Hash() != want.Profile.Hash() || cfg.Identity != want.Identity {
		t.Fatalf("loaded %+v, want %+v", cfg, want)
	}

	link := filepath.Join(dir, "link.json")
	must(t, os.Symlink(good, link))
	hard := write("hard.json", goodConfig, 0o600)
	must(t, os.Link(hard, filepath.Join(dir, "hard2.json")))
	sub := filepath.Join(dir, "sub")
	must(t, os.Mkdir(sub, 0o700))
	bad := map[string]string{
		"group readable": write("open.json", goodConfig, 0o640),
		"symlink":        link,
		"hard link":      hard,
		"directory":      sub,
		"missing":        filepath.Join(dir, "missing.json"),
		"too large":      write("large.json", goodConfig+strings.Repeat(" ", maxConfig), 0o600),
		"unknown field":  write("unknown.json", strings.Replace(goodConfig, `"version": 1,`, `"version": 1, "endpoint": "https://example.com",`, 1), 0o600),
	}
	for name, path := range bad {
		if _, err := LoadConfig(path); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestLoadCredentials(t *testing.T) {
	dir := t.TempDir()
	write := func(name, text string, mode os.FileMode) string {
		p := filepath.Join(dir, name)
		must(t, os.WriteFile(p, []byte(text), mode))
		must(t, os.Chmod(p, mode))
		return p
	}
	const secret = "fixture/secret/never-printed"
	good := write("good.json", `{"access_key_id": "AKIDFIXTURE00000", "secret_access_key": "`+secret+`", "session_token": "tok"}`, 0o600)
	p, err := LoadCredentials(good)
	if err != nil {
		t.Fatal(err)
	}
	creds, err := p.Retrieve(context.Background())
	if err != nil || creds.AccessKeyID != "AKIDFIXTURE00000" || creds.SecretAccessKey != secret || creds.SessionToken != "tok" {
		t.Fatalf("Retrieve: %v", err)
	}

	link := filepath.Join(dir, "link.json")
	must(t, os.Symlink(good, link))
	bad := map[string]string{
		"group readable": write("open.json", `{"access_key_id": "AKIDFIXTURE00000", "secret_access_key": "`+secret+`"}`, 0o640),
		"symlink":        link,
		"missing":        filepath.Join(dir, "missing.json"),
		"unknown field":  write("unknown.json", `{"access_key_id": "AKIDFIXTURE00000", "secret_access_key": "x", "`+secret+`": 1}`, 0o600),
		"bad key":        write("key.json", `{"access_key_id": "akid", "secret_access_key": "`+secret+`"}`, 0o600),
		"no secret":      write("nosecret.json", `{"access_key_id": "AKIDFIXTURE00000", "secret_access_key": ""}`, 0o600),
		"bad secret":     write("space.json", `{"access_key_id": "AKIDFIXTURE00000", "secret_access_key": "a `+secret+`"}`, 0o600),
		"too large":      write("large.json", strings.Repeat(" ", maxCredentials+1), 0o600),
		"trailing data":  write("trailing.json", `{"access_key_id": "AKIDFIXTURE00000", "secret_access_key": "`+secret+`"} {}`, 0o600),
	}
	for name, path := range bad {
		_, err := LoadCredentials(path)
		if err == nil {
			t.Errorf("%s: accepted", name)
			continue
		}
		if strings.Contains(err.Error(), secret) {
			t.Errorf("%s: error quotes the secret: %v", name, err)
		}
	}
}
