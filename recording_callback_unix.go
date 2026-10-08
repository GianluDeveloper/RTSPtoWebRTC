//go:build unix

package main

import (
	"errors"
	"os"
	"os/exec"
	"syscall"
)

// A shell hook can launch other processes. Kill the entire process group on
// timeout/shutdown so a child cannot keep processing an abandoned callback.
func configureRecordingCallbackProcess(command *exec.Cmd) {
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	command.Cancel = func() error {
		err := syscall.Kill(-command.Process.Pid, syscall.SIGKILL)
		if errors.Is(err, syscall.ESRCH) {
			return os.ErrProcessDone
		}
		return err
	}
}
