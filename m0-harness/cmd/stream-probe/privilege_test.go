package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
)

// The drop counter is a word of memory shared with the kernel once mapped:
// reading it needs no system call, so no privilege.
func TestMappedCounterReadsTheSharedWord(t *testing.T) {
	mem := make([]byte, 8)
	binary.LittleEndian.PutUint64(mem, 41)
	c := mappedCounter{mem: mem}
	var n uint64
	if err := c.Lookup(uint32(0), &n); err != nil || n != 41 {
		t.Fatalf("Lookup = %d, %v, want 41", n, err)
	}
	binary.LittleEndian.PutUint64(mem, 42)
	if err := c.Lookup(uint32(0), &n); err != nil || n != 42 {
		t.Fatalf("after the kernel counts one more: %d, %v, want 42", n, err)
	}
	if err := (mappedCounter{mem: make([]byte, 4)}).Lookup(uint32(0), &n); err == nil {
		t.Fatal("a mapping shorter than the counter was read")
	}
}

func TestMapCounterRefusesAMissingMap(t *testing.T) {
	if _, err := mapCounter(nil); err == nil || !strings.Contains(err.Error(), "drops") {
		t.Fatalf("mapCounter(nil) = %v, want an error about the drops map", err)
	}
}

// fakeOS records the identity calls, in order.
type fakeOS struct {
	uid    int
	env    map[string]string
	calls  []string
	failOn string
	status string
}

func (f *fakeOS) install(t *testing.T) {
	t.Helper()
	saved := sysIdentity
	t.Cleanup(func() { sysIdentity = saved })
	step := func(name string) error {
		f.calls = append(f.calls, name)
		if strings.HasPrefix(name, f.failOn) && f.failOn != "" {
			return errors.New("EPERM")
		}
		return nil
	}
	sysIdentity = identity{
		getuid:    func() int { return f.uid },
		getenv:    func(k string) string { return f.env[k] },
		setgroups: func(g []int) error { return step(fmt.Sprintf("setgroups %v", g)) },
		setresgid: func(r, e, s int) error { return step(fmt.Sprintf("setresgid %d %d %d", r, e, s)) },
		setresuid: func(r, e, s int) error { return step(fmt.Sprintf("setresuid %d %d %d", r, e, s)) },
		chown:     func(f2 *os.File, u, g int) error { return step(fmt.Sprintf("chown %d %d", u, g)) },
		status:    func() (string, error) { return f.status, nil },
	}
}

const droppedStatus = "Name:\tstream-probe\nUid:\t1000\t1000\t1000\t1000\nGid:\t1000\t1000\t1000\t1000\nCapEff:\t0000000000000000\n"

func TestDropPrivilegesGivesUpEverythingInOrder(t *testing.T) {
	f := &fakeOS{uid: 0, env: map[string]string{"SUDO_UID": "1000", "SUDO_GID": "1000"}, status: droppedStatus}
	f.install(t)
	if err := dropPrivileges(nil); err != nil {
		t.Fatalf("dropPrivileges = %v", err)
	}
	want := "chown 1000 1000,setgroups [],setresgid 1000 1000 1000,setresuid 1000 1000 1000"
	if got := strings.Join(f.calls, ","); got != want {
		t.Fatalf("calls %s, want %s", got, want)
	}
}

func TestDropPrivilegesFailures(t *testing.T) {
	for name, tc := range map[string]struct {
		f    fakeOS
		want string
	}{
		"root without sudo": {fakeOS{uid: 0, env: map[string]string{}}, "SUDO_UID"},
		"bad SUDO_UID":      {fakeOS{uid: 0, env: map[string]string{"SUDO_UID": "x", "SUDO_GID": "1"}}, "SUDO_UID"},
		"bad SUDO_GID":      {fakeOS{uid: 0, env: map[string]string{"SUDO_UID": "1", "SUDO_GID": ""}}, "SUDO_GID"},
		"sudo to root":      {fakeOS{uid: 0, env: map[string]string{"SUDO_UID": "0", "SUDO_GID": "0"}}, "root"},
		"setresuid refused": {fakeOS{uid: 0, env: map[string]string{"SUDO_UID": "1000", "SUDO_GID": "1000"}, failOn: "setresuid"}, "setresuid"},
		"setgroups refused": {fakeOS{uid: 0, env: map[string]string{"SUDO_UID": "1000", "SUDO_GID": "1000"}, failOn: "setgroups"}, "setgroups"},
		"capabilities left": {fakeOS{uid: 0, env: map[string]string{"SUDO_UID": "1000", "SUDO_GID": "1000"},
			status: strings.Replace(droppedStatus, "0000000000000000", "0000000000000400", 1)}, "capabilit"},
		"status unreadable as such": {fakeOS{uid: 0, env: map[string]string{"SUDO_UID": "1000", "SUDO_GID": "1000"},
			status: "Name:\tstream-probe\n"}, "Uid and CapEff"},
		"still root": {fakeOS{uid: 0, env: map[string]string{"SUDO_UID": "1000", "SUDO_GID": "1000"},
			status: strings.Replace(droppedStatus, "Uid:\t1000\t1000\t1000\t1000", "Uid:\t1000\t0\t1000\t1000", 1)}, "uid"},
	} {
		t.Run(name, func(t *testing.T) {
			f := tc.f
			f.install(t)
			if err := dropPrivileges(nil); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("dropPrivileges = %v, want an error about %s", err, tc.want)
			}
		})
	}
}

// Already unprivileged (tests, CI without sudo): nothing to drop.
func TestDropPrivilegesIsANoOpWhenNotRoot(t *testing.T) {
	f := &fakeOS{uid: 1000}
	f.install(t)
	if err := dropPrivileges(nil); err != nil || len(f.calls) != 0 {
		t.Fatalf("dropPrivileges = %v, calls %v, want nothing done", err, f.calls)
	}
}

func TestRunDropsPrivilegesAfterSettingUp(t *testing.T) {
	withRunSeams(t)
	f := &fakeOS{uid: 0, env: map[string]string{"SUDO_UID": "1000", "SUDO_GID": "1000"}, status: droppedStatus}
	f.install(t)
	if err := run(); err != nil {
		t.Fatalf("run() = %v", err)
	}
	if len(f.calls) != 4 {
		t.Fatalf("calls %v, want the four identity steps", f.calls)
	}
	f2 := &fakeOS{uid: 0, env: map[string]string{}}
	f2.install(t)
	if err := run(); err == nil || !strings.Contains(err.Error(), "drop privileges") {
		t.Fatalf("run() = %v, want a drop privileges error", err)
	}
}
