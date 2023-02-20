package state

import (
	"encoding/json"
	"fmt"
)

func MarshalEvent(event any) ([]byte, error) {
	data, err := json.Marshal(event)
	if err != nil {
		return nil, fmt.Errorf("json marshal: %w", err)
	}
	return data, nil
}

func UnmarshalEvent(data []byte, event any) error {
	err := json.Unmarshal(data, event)
	if err != nil {
		return fmt.Errorf("json unmarshal: %w", err)
	}
	return nil
}
