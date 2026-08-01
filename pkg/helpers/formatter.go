package helpers

import (
	"fmt"
	"runtime"
	"strings"
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
	callr := ""
	_, file, line, ok := runtime.Caller(1)
	if ok {
		callr = fmt.Sprintf("%s:%d", file, line)
	}
	t.Fatal(BOLDRED + callr + " ✗ " + t.Name() + " " + msg + RESET)
}

// PrintBPTreeNode renders a B+Tree node as an ASCII table and returns it as a string(Use only when testing).
//
// Usage:
//   - Leaf node:     pass keys and values (both [][]byte), pointers = nil.
//     len(keys) must equal len(values).
//   - Internal node: pass keys and pointers ([]uint32), values = nil.
//     len(pointers) must equal len(keys)+1 (the rightmost
//     pointer occupies the rightmost column, with no key
//     above it). A pointer value of 0 is rendered as an
//     empty cell (no page/child is assumed to exist).
//
// Exactly one of values/pointers should be provided (non-nil); if both or
// neither are given, PrintBPTreeNode returns an error message string.
func PrintBPTreeNode(keys [][]byte, values [][]byte, pointers []uint32) string {
	haveValues := values != nil
	havePointers := pointers != nil

	if haveValues == havePointers {
		return "error: provide exactly one of values or pointers"
	}

	var top, bottom []string

	if havePointers {
		// Internal node: N keys, N+1 pointers.
		if len(pointers) != len(keys)+1 {
			return fmt.Sprintf("error: internal node needs len(pointers) == len(keys)+1, got %d keys and %d pointers",
				len(keys), len(pointers))
		}
		top = make([]string, len(pointers))
		for i, k := range keys {
			top[i] = string(k)
		}
		top[len(pointers)-1] = "" // last column of the key row has no key

		bottom = make([]string, len(pointers))
		for i, p := range pointers {
			if p == 0 {
				bottom[i] = ""
			} else {
				bottom[i] = fmt.Sprintf("%d", p)
			}
		}
	} else {
		// Leaf node: N keys, N values.
		if len(values) != len(keys) {
			return fmt.Sprintf("error: leaf node needs len(values) == len(keys), got %d keys and %d values",
				len(keys), len(values))
		}
		top = make([]string, len(keys))
		bottom = make([]string, len(keys))
		for i := range keys {
			top[i] = string(keys[i])
			bottom[i] = string(values[i])
		}
	}

	n := len(top)
	if n == 0 {
		return "+--+\n|  |\n+--+"
	}

	// Compute a column width for each cell: wide enough for the taller of
	// the top/bottom content, plus one space of padding on each side.
	widths := make([]int, n)
	for i := 0; i < n; i++ {
		w := len(top[i])
		if len(bottom[i]) > w {
			w = len(bottom[i])
		}
		w += 2
		if w < 4 {
			w = 4
		}
		widths[i] = w
	}

	// Build the horizontal border line, e.g. "+----+-----+----+".
	var borderBuilder strings.Builder
	borderBuilder.WriteByte('+')
	for _, w := range widths {
		borderBuilder.WriteString(strings.Repeat("-", w))
		borderBuilder.WriteByte('+')
	}
	border := borderBuilder.String()

	// buildRow renders one content row (either the keys or the
	// values/pointers) padded out to the computed column widths.
	buildRow := func(cells []string) string {
		var rowBuilder strings.Builder
		rowBuilder.WriteByte('|')
		for i, c := range cells {
			pad := widths[i] - len(c) - 1 // leading space + content + trailing pad
			if pad < 1 {
				pad = 1
			}
			rowBuilder.WriteByte(' ')
			rowBuilder.WriteString(c)
			rowBuilder.WriteString(strings.Repeat(" ", pad))
			rowBuilder.WriteByte('|')
		}
		return rowBuilder.String()
	}

	var sb strings.Builder
	sb.WriteString(border)
	sb.WriteByte('\n')
	sb.WriteString(buildRow(top))
	sb.WriteByte('\n')
	sb.WriteString(border)
	sb.WriteByte('\n')
	sb.WriteString(buildRow(bottom))
	sb.WriteByte('\n')
	sb.WriteString(border)

	return sb.String()
}
