package auth_test

import (
	"errors"
	"fmt"

	"github.com/tmc/skygo/internal/auth"
)

func ExampleDelegation() {
	// A zero delegation identifies no credential and grants no authority.
	var value auth.Delegation
	fmt.Println(value.Principal == "", value.TokenID == "", value.Digest == ([32]byte{}))
	// Output: true true true
}

func ExampleEngine_Delegate() {
	// Ordinary anonymous local authority cannot be delegated to data workers.
	engine := auth.NewLegacy("")
	actor, err := engine.Authenticate("")
	if err != nil {
		panic(err)
	}
	_, err = engine.Delegate(actor)
	fmt.Println(errors.Is(err, auth.ErrUnauthenticated))
	// Output: true
}

func ExampleEngine_CheckDelegation() {
	// A legacy engine cannot check persisted file-credential delegation.
	err := auth.NewLegacy("").CheckDelegation(auth.Delegation{})
	fmt.Println(errors.Is(err, auth.ErrUnauthenticated))
	// Output: true
}
