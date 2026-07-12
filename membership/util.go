package membership

import (
	"net"
)

func IsUnspecifiedHost(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return false
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsUnspecified()
}

func DeriveEventType(wasPresent bool, oldState, newState State) EventType {
	if !wasPresent {
		return Join
	}
	switch newState {
	case Left:
		if oldState != Left {
			return Leave
		}
	case Failed:
		if oldState != Failed {
			return Fail
		}
	}
	return Update
}
