package artifact

import (
	"errors"
	"fmt"
)

func ExampleFileCloseError() {
	// An ordinary policy conflict has no attempted original Close to retain.
	fmt.Println(errors.Is(ErrFileConflict, ErrFileCloseUncertain))
	// The real adapter keeps its original slot/owner when the sentinel appears
	// in an operation's wrapped error; it never reuses a raw descriptor number.
	fmt.Println(errors.Is(errors.Join(ErrFileConflict, ErrFileCloseUncertain), ErrFileCloseUncertain))
	// Output:
	// false
	// true
}
