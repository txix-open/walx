package replication

import (
	"github.com/txix-open/walx"
	"github.com/txix-open/walx/replication/replicator"
	"github.com/txix-open/walx/state"
)

type streamMatcher struct {
	matchAllStreams        bool
	filteredStreamsInBytes [][]byte
}

func newStreamMatcher(request *replicator.BeginRequest) streamMatcher {
	matchAllStreams := false
	filteredStreamsInBytes := make([][]byte, 0)
	for _, stream := range request.GetFilteredStreams() {
		if stream == AllStreams {
			matchAllStreams = true
		}
		filteredStreamsInBytes = append(filteredStreamsInBytes, []byte(stream))
	}
	return streamMatcher{
		matchAllStreams:        matchAllStreams,
		filteredStreamsInBytes: filteredStreamsInBytes,
	}
}

func (m streamMatcher) Match(entry walx.Entry) bool {
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
