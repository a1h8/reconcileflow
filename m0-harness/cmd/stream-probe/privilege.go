// Least privilege: started through sudo, stream-probe does what needs root
// (load, attach, open the ring buffer, map the drop counter, create its
// output), then gives everything up before reading a single event: no
// supplementary groups, real, effective and saved ids those of the sudo
// user, hence no capability left. It checks /proc/self/status and stops
// if anything remains.
package main

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"unsafe"

	"github.com/cilium/ebpf"
	"golang.org/x/sys/unix"
)

// mappedCounter reads the drop counter from the drops map mapped into this
// process (BPF_F_MMAPABLE): a word the kernel increments, read without any
// system call, so without privilege.
type mappedCounter struct {
	mem []byte
}

func (c mappedCounter) Lookup(_, valueOut interface{}) error {
	if len(c.mem) < 8 {
		return errors.New("drop counter mapping shorter than 8 bytes")
	}
	// Host byte order, as the kernel wrote it; page-aligned, so atomic.
	*(valueOut.(*uint64)) = atomic.LoadUint64((*uint64)(unsafe.Pointer(&c.mem[0])))
	return nil
}

// mapCounter maps the drops map's single value into memory, while still
// privileged.
func mapCounter(m *ebpf.Map) (dropCounter, error) {
	if m == nil {
		return nil, errors.New("no drops map in the BPF object")
	}
	mem, err := unix.Mmap(m.FD(), 0, os.Getpagesize(), unix.PROT_READ, unix.MAP_SHARED)
	if err != nil {
		return nil, fmt.Errorf("map the drops counter (needs BPF_F_MMAPABLE): %w", err)
	}
	return mappedCounter{mem: mem}, nil
}

// identity holds the process identity calls; tests replace them.
type identity struct {
	getuid    func() int
	getenv    func(string) string
	setgroups func([]int) error
	setresgid func(r, e, s int) error
	setresuid func(r, e, s int) error
	chown     func(f *os.File, uid, gid int) error
	status    func() (string, error)
}

// syscall's Set* apply to every thread of the process (Go >= 1.16).
var sysIdentity = identity{
	getuid:    os.Getuid,
	getenv:    os.Getenv,
	setgroups: syscall.Setgroups,
	setresgid: syscall.Setresgid,
	setresuid: syscall.Setresuid,
	chown:     func(f *os.File, uid, gid int) error { return f.Chown(uid, gid) },
	status: func() (string, error) {
		b, err := os.ReadFile("/proc/self/status")
		return string(b), err
	},
}

// dropPrivileges hands out to the sudo user, then becomes that user for
// good. A no-op when not root.
func dropPrivileges(out *os.File) error {
	id := sysIdentity
	if id.getuid() != 0 {
		return nil
	}
	uid, err := strconv.Atoi(id.getenv("SUDO_UID"))
	if err != nil {
		return errors.New("running as root without a valid SUDO_UID: start stream-probe through sudo")
	}
	gid, err := strconv.Atoi(id.getenv("SUDO_GID"))
	if err != nil {
		return errors.New("running as root without a valid SUDO_GID: start stream-probe through sudo")
	}
	if uid == 0 {
		return errors.New("SUDO_UID is root: nothing to drop to")
	}
	if err := id.chown(out, uid, gid); err != nil {
		return fmt.Errorf("give the output to the sudo user: %w", err)
	}
	if err := id.setgroups([]int{}); err != nil {
		return fmt.Errorf("setgroups: %w", err)
	}
	if err := id.setresgid(gid, gid, gid); err != nil {
		return fmt.Errorf("setresgid: %w", err)
	}
	if err := id.setresuid(uid, uid, uid); err != nil {
		return fmt.Errorf("setresuid: %w", err)
	}
	return checkDropped(id, uid)
}

// checkDropped reads the kernel's view: every uid the sudo user's, no
// effective capability.
func checkDropped(id identity, uid int) error {
	status, err := id.status()
	if err != nil {
		return fmt.Errorf("read the process status: %w", err)
	}
	want := strconv.Itoa(uid)
	seen := 0
	for _, line := range strings.Split(status, "\n") {
		key, value, _ := strings.Cut(line, ":")
		fields := strings.Fields(value)
		switch key {
		case "Uid":
			seen++
			for _, f := range fields {
				if f != want {
					return fmt.Errorf("a uid is still %s after dropping to %d: %s", f, uid, value)
				}
			}
		case "CapEff":
			seen++
			if len(fields) != 1 || strings.Trim(fields[0], "0") != "" {
				return fmt.Errorf("effective capabilities left after dropping: %s", value)
			}
		}
	}
	if seen != 2 {
		return errors.New("process status without Uid and CapEff lines: cannot check the drop")
	}
	return nil
}
