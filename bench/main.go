package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	rand2 "math/rand"
	"net/http"
	_ "net/http/pprof"
	"os"
	"sync"
	"time"

	"github.com/julienschmidt/httprouter"
	"github.com/txix-open/isp-kit/http/endpoint"
	"github.com/txix-open/isp-kit/http/endpoint/httplog"
	"github.com/txix-open/isp-kit/log"
	"github.com/txix-open/walx/v2"
	"github.com/txix-open/walx/v2/state"
	"github.com/txix-open/walx/v2/state/codec/json"
)

type SaveData struct {
	Key   string
	Value string
}

type events struct {
	SaveData *SaveData
}

type service struct {
	lock    *sync.RWMutex
	storage map[string]string
}

func newService() *service {
	return &service{
		storage: map[string]string{},
		lock:    &sync.RWMutex{},
	}
}

func (s *service) SaveData(data SaveData) (any, error) {
	s.storage[data.Key] = data.Value
	return data.Value, nil
}

func (s *service) Get(key string) string {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return s.storage[key]
}

type controller struct {
	s *service
}

func (c controller) Apply(log state.Log) (any, error) {
	e, err := state.UnmarshalEvent[events](log)
	if err != nil {
		return nil, err
	}

	c.s.lock.Lock()
	defer c.s.lock.Unlock()

	switch {
	case e.SaveData != nil:
		return c.s.SaveData(*e.SaveData)
	default:
		return nil, errors.New("unknown event")
	}
}

func main() {
	logger, err := log.New(log.WithLevel(log.InfoLevel))
	if err != nil {
		panic(err)
	}

	service := newService()
	controller := controller{s: service}

	dir := dir()
	defer func() {
		os.RemoveAll(dir)
	}()
	wal, err := walx.Open(dir)
	if err != nil {
		panic(err)
	}
	state := state.New(wal, controller, json.NewCodec(), "test")
	defer state.Close()
	err = state.Recovery(context.Background())
	if err != nil {
		panic(err)
	}
	go func() {
		err := state.Run(context.Background())
		if err != nil {
			panic(err)
		}
	}()

	for i := 0; i < 128; i++ {
		go func() {
			min := 21
			max := 64
			rnd := rand2.New(rand2.NewSource(time.Now().UnixNano()))
			for {
				latency := rnd.Intn(max-min) + min
				service.Get("some key")
				time.Sleep(time.Duration(latency) * time.Millisecond)
			}
		}()
	}

	wrapper := endpoint.DefaultWrapper(logger, httplog.Noop())
	handler := wrapper.EndpointV2(endpoint.New(func(ctx context.Context, req SaveData) (string, error) {
		s, err := state.Apply(events{SaveData: &req}, nil)
		if err != nil {
			return "", err
		}
		return s.(string), nil
	}))
	/*
		go func() {
			time.Sleep(3 * time.Second)
			f, err := os.Create("profile.out")
			if err != nil {
				panic(err)
			}
			defer f.Close()
			err = trace.Start(f)
			if err != nil {
				panic(err)
			}
			defer trace.Stop()
			time.Sleep(30 * time.Second)
		}()
	*/
	/*
		handler := func(writer http.ResponseWriter, request *http.Request) {
			val := SaveData{}
			err := json.NewDecoder(request.Body).Decode(&val)
			if err != nil {
				panic(err)
			}
			s, err := state.Apply(events{SaveData: &val}, nil)
			if err != nil {
				panic(err)
			}
			writer.Write([]byte(s.(string)))
		}*/
	go func() {
		http.ListenAndServe("localhost:6060", nil)
	}()

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
