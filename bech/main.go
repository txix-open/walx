package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"net/http"
	"os"

	"github.com/integration-system/isp-kit/http/endpoint"
	"github.com/integration-system/isp-kit/log"
	"github.com/julienschmidt/httprouter"
	"walx"
	"walx/state"
)

type SaveData struct {
	Key   string
	Value string
}

type events struct {
	SaveData *SaveData
}

type service struct {
	storage map[string]string
}

func (s *service) SaveData(data SaveData) (any, error) {
	s.storage[data.Key] = data.Value
	return data.Value, nil
}

type controller struct {
	s service
}

func (c controller) Apply(log []byte) (any, error) {
	e := events{}
	err := state.UnmarshalEvent(log, &e)
	if err != nil {
		return nil, err
	}
	switch {
	case e.SaveData != nil:
		return c.s.SaveData(*e.SaveData)
	default:
		return nil, errors.New("unknown event")
	}
}

func main() {
	logger, err := log.New(log.WithLevel(log.DebugLevel))
	if err != nil {
		panic(err)
	}

	service := service{storage: map[string]string{}}
	controller := controller{s: service}

	dir := dir()
	defer func() {
		os.RemoveAll(dir)
	}()
	wal, err := walx.Open(dir)
	if err != nil {
		panic(err)
	}
	state := state.New(wal, controller)
	defer state.Close()
	err = state.Recovery()
	if err != nil {
		panic(err)
	}
	go func() {
		err := state.Run()
		if err != nil {
			panic(err)
		}
	}()

	wrapper := endpoint.DefaultWrapper(logger)
	handler := wrapper.Endpoint(func(ctx context.Context, req SaveData) (string, error) {
		s, err := state.Apply(events{SaveData: &req})
		if err != nil {
			return "", err
		}
		return s.(string), nil
	})
	router := httprouter.New()
	router.HandlerFunc(http.MethodPost, "/api/handler", handler)

	ss := http.Server{
		Addr:    "127.0.0.1:8000",
		Handler: router,
	}
	err = ss.ListenAndServe()
	if errors.Is(err, http.ErrServerClosed) {
		return
	}
	if err != nil {
		panic(err)
	}
}

func dir() string {
	d := make([]byte, 8)
	_, _ = rand.Read(d)
	return hex.EncodeToString(d)
}
