//go:build !unix

package main

import "os/exec"

func configureRecordingCallbackProcess(command *exec.Cmd) {}
