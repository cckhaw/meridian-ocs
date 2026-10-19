// Package lifecycle is the subscriber lifecycle state machine
// (Active -> Grace -> Suspended -> Terminated, with recovery paths).
//
// The authoritative copy of these rules runs inside the provisioning script in
// Redis (pkg/datalayer/lua/provision.lua), where the check and the state
// change are one atomic step; this package is the same table for early
// validation, documentation and tests, and a test asserts the two agree.
package lifecycle

// State is a subscriber lifecycle state.
type State uint8

const (
	Unknown State = iota
	Active
	Grace
	Suspended
	Terminated
)

var names = [...]string{Unknown: "unknown", Active: "active", Grace: "grace", Suspended: "suspended", Terminated: "terminated"}

func (s State) String() string {
	if int(s) < len(names) {
		return names[s]
	}
	return "unknown"
}

// Parse converts a state name.
func Parse(s string) (State, bool) {
	for st := Active; st <= Terminated; st++ {
		if names[st] == s {
			return st, true
		}
	}
	return Unknown, false
}

// transitions lists the states reachable from each state.
//
//	Active     -> Grace, Suspended (fraud / admin barring), Terminated
//	Grace      -> Active (paid), Suspended (grace expired), Terminated
//	Suspended  -> Active (reinstated), Terminated
//	Terminated -> nothing: terminal
var transitions = map[State][]State{
	Active:     {Grace, Suspended, Terminated},
	Grace:      {Active, Suspended, Terminated},
	Suspended:  {Active, Terminated},
	Terminated: nil,
}

// CanTransition reports whether from -> to is allowed. Staying in the same
// state is not a transition.
func CanTransition(from, to State) bool {
	for _, s := range transitions[from] {
		if s == to {
			return true
		}
	}
	return false
}

// Next returns the states reachable from s.
func Next(s State) []State { return append([]State(nil), transitions[s]...) }

// Serves reports whether a subscriber in this state may be granted new
// service quota. Grace still serves (that is its purpose); Suspended and
// Terminated do not. An unknown state is treated as serving so that
// subscribers provisioned before profiles existed keep working.
func Serves(s State) bool { return s != Suspended && s != Terminated }
