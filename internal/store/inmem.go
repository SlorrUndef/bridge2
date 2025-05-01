package store

import (
	"sync"
	"time"

	"github.com/rs/zerolog/log"
	"tonconnect-bridge/internal/bridge"
)

const (
	maxEvents       = 32
	retentionPeriod = 30 * time.Second
)

type Storage struct {
	events     []*bridge.Event
	deliveries map[uint64]time.Time
	mu         sync.Mutex
}

func NewStorage() *Storage {
	return &Storage{
		events:     make([]*bridge.Event, 0, maxEvents),
		deliveries: make(map[uint64]time.Time),
	}
}

func (s *Storage) Push(event *bridge.Event) bool {
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.events) < maxEvents {
		s.events = append(s.events, event)
		return true
	}

	idx := 0
	for _, ev := range s.events {
		deliveryTime, wasDelivered := s.deliveries[ev.ID]

		keepEvent := !ev.IsExpired(now)
		if wasDelivered {
			keepEvent = keepEvent && now.Sub(deliveryTime) < retentionPeriod
		}

		if keepEvent {
			s.events[idx] = ev
			idx++
		} else if wasDelivered {
			delete(s.deliveries, ev.ID)
		}
	}

	s.events = s.events[:idx]

	if len(s.events) < maxEvents {
		s.events = append(s.events, event)
		return true
	}

	return false
}

func (s *Storage) ExecuteAll(lastEventId uint64, exec func(event *bridge.Event) error) error {
	now := time.Now()

	s.mu.Lock()
	defer s.mu.Unlock()

	for _, e := range s.events {
		if e.IsExpired(now) || e.ID <= lastEventId {
			continue
		}

		if err := exec(e); err != nil {
			return err
		}

		s.deliveries[e.ID] = now

		log.Debug().Uint64("id", e.ID).Msg("event delivered")
	}

	return nil
}
