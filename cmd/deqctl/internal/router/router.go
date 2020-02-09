package router

import (
	"context"
	"fmt"
	"io"
)

// Router stores and retrieves routes by command names. Router should be created
// by calling NewRouter.
type Router struct {
	commands map[string]Command
}

// New creates a new Router with no routes registered.
func New() *Router {
	return &Router{
		commands: make(map[string]Command),
	}
}

// Register adds cmd to the router. Register panics if any of cmd's names have
// already been registered.
func (r *Router) Register(cmd Command) {
	for _, n := range cmd.Names() {
		if _, ok := r.commands[n]; ok {
			panic(fmt.Sprintf("a command with name %q has already been registered", n))
		}
		r.commands[n] = cmd
	}
}

// Get returns the route registered for cmd, or nil if no route is
// registered for cmd.
func (r *Router) Get(cmd string) Command {
	return r.commands[cmd]
}

// Usage writes a usage string for each command to w.
func (r *Router) Usage(w io.Writer) {
	for _, command := range r.commands {
		fmt.Fprintf(w, "%s: %s", command.DisplayName(), command.Summary())
	}
}

// Command is a command that can be retrieved from a router and run.
type Command interface {
	DisplayName() string
	Names() []string
	Summary() string
	Run(ctx context.Context, args []string) error
}

// ErrorCode indicates the code of an error returned by running a command.
type ErrorCode int

const (
	// Invalid indicates an error was caused because a command was run with
	// invalid args.
	Invalid ErrorCode = iota
)

type runError struct {
	code ErrorCode
	msg  string
}

func (err *runError) Error() string {
	return err.msg
}

// Errorf returns an error containing code.
func Errorf(code ErrorCode, format string, a ...interface{}) error {
	return &runError{
		code: code,
		msg:  fmt.Sprintf(format, a...),
	}
}
