package helpers

import (
	"fmt"
	"testing"
)

const (
	BOLDRED    = "\033[1m\033[31m"
	BOLDGREEN  = "\033[1m\033[32m"
	BOLDYELLOW = "\033[1m\033[33m"
	BOLDBLUE   = "\033[1m\033[34m"
	RESET      = "\033[0m"
	BOLDCYAN   = "\033[1m\033[36m"
	BOLDWHITE  = "\033[1m\033[37m"
)

// PrintErrorMsg formats an error message with the appropriate color
func PrintErrorMsg(msg string) {
	fmt.Println(BOLDRED + " ✗ " + msg + RESET)
}

// PrintSuccessMsg formats a success message with the appropriate color
func PrintSuccessMsg(msg string) {
	fmt.Println(BOLDGREEN + "  ✓ " + msg + RESET)
}

// PrintInfoMsg formats an info message with the appropriate color
func PrintInfoMsg(msg string) {
	fmt.Println(BOLDCYAN + "  💡 " + msg + RESET)
}

// PrintTestErrorMsg formats a test error message with the appropriate color
func PrintTestErrorMsg(msg string, t *testing.T) {
	t.Fatal(BOLDRED + " ✗ " + t.Name() + " " + msg + RESET)
}
