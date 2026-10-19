package auth_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/tmc/skygo/internal/auth"
)

func exampleFiles() (string, string, func()) {
	dir, err := os.MkdirTemp("", "skygo-auth-example-")
	if err != nil {
		panic(err)
	}
	path := filepath.Join(dir, "auth.json")
	token := filepath.Join(dir, "token")
	if _, err = auth.Initialize(path, token, "0123456789abcdef0123456789abcdef"); err != nil {
		os.RemoveAll(dir)
		panic(err)
	}
	return path, token, func() { os.RemoveAll(dir) }
}

func Example() {
	engine := auth.NewLegacy("operator-token")
	actor, err := engine.Authenticate("Bearer operator-token")
	if err != nil {
		panic(err)
	}
	actor, err = engine.Check(actor, auth.Mutate)
	if err != nil {
		panic(err)
	}
	fmt.Println(actor.Principal(), actor.Role())
	// Output: legacy admin
}

func ExampleNewLegacy() {
	engine := auth.NewLegacy("") // Explicit single-operator anonymous mode.
	actor, err := engine.Authenticate("")
	if err != nil {
		panic(err)
	}
	fmt.Println(actor.Principal())
	// Output: legacy
}

func ExampleNewFile() {
	path, _, done := exampleFiles()
	defer done()
	engine, err := auth.NewFile(path, "0123456789abcdef0123456789abcdef")
	if err != nil {
		panic(err)
	}
	fmt.Println(engine.Ready())
	// Output: true
}

func ExampleEngine_Authenticate() {
	engine := auth.NewLegacy("operator-token")
	actor, err := engine.Authenticate("Bearer operator-token")
	if err != nil {
		panic(err)
	}
	fmt.Println(actor.Principal())
	// Output: legacy
}

func ExampleEngine_Check() {
	engine := auth.NewLegacy("")
	actor, err := engine.Authenticate("")
	if err != nil {
		panic(err)
	}
	_, err = engine.Check(actor, auth.Read)
	fmt.Println(err)
	// Output: <nil>
}

func ExampleEngine_Ready() {
	fmt.Println(auth.NewLegacy("").Ready()) // Output: true
}

func ExampleEngine_Changed() {
	select {
	case <-auth.NewLegacy("").Changed():
		fmt.Println("changed")
	default:
		fmt.Println("unchanged")
	}
	// Output: unchanged
}

func ExampleEngine_Run() {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	auth.NewLegacy("").Run(ctx)
	fmt.Println("stopped")
	// Output: stopped
}

func ExampleActor_Principal() {
	actor, err := auth.NewLegacy("").Authenticate("")
	if err != nil {
		panic(err)
	}
	fmt.Println(actor.Principal())
	// Output: legacy
}

func ExampleActor_Role() {
	actor, err := auth.NewLegacy("").Authenticate("")
	if err != nil {
		panic(err)
	}
	fmt.Println(actor.Role())
	// Output: admin
}

func ExampleActor_TokenID() {
	path, tokenFile, done := exampleFiles()
	defer done()
	engine, err := auth.NewFile(path, "0123456789abcdef0123456789abcdef")
	if err != nil {
		panic(err)
	}
	token, err := os.ReadFile(tokenFile)
	if err != nil {
		panic(err)
	}
	actor, err := engine.Authenticate("Bearer " + strings.TrimSpace(string(token)))
	if err != nil {
		panic(err)
	}
	fmt.Println(len(actor.TokenID())) // The ID is public; the bearer token is secret.
	// Output: 32
}

func ExampleInitialize() {
	path, _, done := exampleFiles()
	defer done()
	tokens, err := auth.Tokens(path)
	if err != nil {
		panic(err)
	}
	fmt.Println(tokens[0].Principal)
	// Output: legacy
}

func ExampleAddPrincipal() {
	path, _, done := exampleFiles()
	defer done()
	err := auth.AddPrincipal(path, "alice", auth.Operator)
	fmt.Println(err)
	// Output: <nil>
}

func ExampleSetRole() {
	path, _, done := exampleFiles()
	defer done()
	if err := auth.AddPrincipal(path, "alice", auth.Operator); err != nil {
		panic(err)
	}
	fmt.Println(auth.SetRole(path, "alice", auth.Reader))
	// Output: <nil>
}

func ExampleIssue() {
	path, _, done := exampleFiles()
	defer done()
	output := filepath.Join(filepath.Dir(path), "rotated.token")
	info, err := auth.Issue(path, "legacy", output, time.Hour)
	if err != nil {
		panic(err)
	}
	fmt.Println(info.Principal, len(info.ID)) // Never print the bearer token.
	// Output: legacy 32
}

func ExampleRevoke() {
	path, _, done := exampleFiles()
	defer done()
	old, err := auth.Tokens(path)
	if err != nil {
		panic(err)
	}
	if _, err = auth.Issue(path, "legacy", filepath.Join(filepath.Dir(path), "rotated.token"), time.Hour); err != nil {
		panic(err)
	}
	fmt.Println(auth.Revoke(path, old[0].ID))
	// Output: <nil>
}

func ExampleTokens() {
	path, _, done := exampleFiles()
	defer done()
	tokens, err := auth.Tokens(path)
	if err != nil {
		panic(err)
	}
	fmt.Println(len(tokens), tokens[0].Principal)
	// Output: 1 legacy
}
