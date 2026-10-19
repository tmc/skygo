package control_test

import "github.com/tmc/skygo/internal/auth"

var testEngine = auth.NewLegacy("")

func testActor() auth.Actor {
	a, err := testEngine.Authenticate("")
	if err != nil {
		panic(err)
	}
	return a
}
