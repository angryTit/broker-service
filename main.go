package main

import (
	"fmt"
	"net/http"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
)

type queue struct {
	msgs    []string
	waiters []chan string
}

type broker struct {
	mu     sync.Mutex
	queues map[string]*queue
}

func newBroker() *broker {
	return &broker{queues: make(map[string]*queue)}
}

func (b *broker) ensureQueue(name string) *queue {
	q, ok := b.queues[name]
	if !ok {
		q = &queue{}
		b.queues[name] = q
	}
	return q
}

func (b *broker) push(name, msg string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	q := b.ensureQueue(name)
	if len(q.waiters) > 0 {
		ch := q.waiters[0]
		q.waiters = q.waiters[1:]
		ch <- msg
		return
	}
	q.msgs = append(q.msgs, msg)
}

func (b *broker) pop(name string, timeout time.Duration) (string, bool) {
	b.mu.Lock()
	q := b.ensureQueue(name)
	if len(q.msgs) > 0 {
		msg := q.msgs[0]
		q.msgs = q.msgs[1:]
		b.mu.Unlock()
		return msg, true
	}
	if timeout <= 0 {
		b.mu.Unlock()
		return "", false
	}
	ch := make(chan string, 1)
	q.waiters = append(q.waiters, ch)
	b.mu.Unlock()

	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case msg := <-ch:
		return msg, true
	case <-timer.C:
		b.mu.Lock()
		for i, w := range q.waiters {
			if w == ch {
				q.waiters = slices.Delete(q.waiters, i, i+1)
				b.mu.Unlock()
				return "", false
			}
		}
		b.mu.Unlock()
		return <-ch, true
	}
}

func (b *broker) handle(w http.ResponseWriter, r *http.Request) {
	name := strings.SplitN(strings.TrimPrefix(r.URL.Path, "/"), "/", 2)[0]
	if name == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	params := r.URL.Query()
	switch r.Method {
	case http.MethodPut:
		if !params.Has("v") {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		b.push(name, params.Get("v"))
	case http.MethodGet:
		var timeout time.Duration
		if n, err := strconv.Atoi(params.Get("timeout")); err == nil && n > 0 {
			timeout = time.Duration(n) * time.Second
		}
		msg, ok := b.pop(name, timeout)
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		fmt.Fprint(w, msg)
	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: broker-service <port>")
		os.Exit(1)
	}
	port, err := strconv.Atoi(os.Args[1])
	if err != nil || port < 1 || port > 65535 {
		fmt.Fprintln(os.Stderr, "invalid port")
		os.Exit(1)
	}
	b := newBroker()
	http.HandleFunc("/", b.handle)
	if err := http.ListenAndServe(fmt.Sprintf(":%d", port), nil); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
