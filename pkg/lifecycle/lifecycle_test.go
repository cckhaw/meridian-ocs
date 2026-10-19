package lifecycle

import "testing"

func TestTransitionTable(t *testing.T) {
	want := map[[2]State]bool{
		{Active, Grace}: true, {Active, Suspended}: true, {Active, Terminated}: true,
		{Grace, Active}: true, {Grace, Suspended}: true, {Grace, Terminated}: true,
		{Suspended, Active}: true, {Suspended, Terminated}: true,
	}
	for from := Active; from <= Terminated; from++ {
		for to := Active; to <= Terminated; to++ {
			if got := CanTransition(from, to); got != want[[2]State{from, to}] {
				t.Errorf("%v -> %v: got %v", from, to, got)
			}
		}
	}
	if CanTransition(Unknown, Active) || CanTransition(Active, Unknown) {
		t.Error("Unknown is not a state to move to or from")
	}
}

func TestTerminatedIsTerminal(t *testing.T) {
	if len(Next(Terminated)) != 0 {
		t.Fatal("nothing may follow Terminated")
	}
}

func TestParseRoundTrip(t *testing.T) {
	for s := Active; s <= Terminated; s++ {
		if got, ok := Parse(s.String()); !ok || got != s {
			t.Errorf("%v", s)
		}
	}
	if _, ok := Parse("Active"); ok {
		t.Error("names are lower case")
	}
	if _, ok := Parse("unknown"); ok {
		t.Error("unknown is not a settable state")
	}
}

func TestServes(t *testing.T) {
	for s, want := range map[State]bool{Active: true, Grace: true, Suspended: false, Terminated: false, Unknown: true} {
		if Serves(s) != want {
			t.Errorf("%v", s)
		}
	}
}

func TestNextReturnsACopy(t *testing.T) {
	n := Next(Active)
	n[0] = Terminated
	if Next(Active)[0] != Grace {
		t.Fatal("callers must not be able to alter the table")
	}
}
