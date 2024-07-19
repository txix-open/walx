package stream

import (
	"github.com/txix-open/walx"
	"github.com/txix-open/walx/state"
)

const (
	AllStreams = "*"
)

type Matcher struct {
	matchAllStreams        bool
	filteredStreamsInBytes [][]byte
}

func NewMatcher(filteredStreams []string) Matcher {
	matchAllStreams := false
	filteredStreamsInBytes := make([][]byte, 0)
	for _, stream := range filteredStreams {
		if stream == AllStreams {
			matchAllStreams = true
		}
		filteredStreamsInBytes = append(filteredStreamsInBytes, []byte(stream))
	}
	return Matcher{
		matchAllStreams:        matchAllStreams,
		filteredStreamsInBytes: filteredStreamsInBytes,
	}
}

func (m Matcher) Match(entry walx.Entry) bool {
	if m.matchAllStreams {
		return true
	}

	streamName, _ := state.UnpackEvent(entry.Data)
	for _, s2 := range m.filteredStreamsInBytes {
		if state.MatchStream(streamName, s2) {
			return true
		}
	}

	return false
}
