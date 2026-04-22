package web

import (
	"io"
	"os"
)

// errLogger is where Serve prints lifecycle messages. Overridable for tests.
var errLogger io.Writer = os.Stderr
