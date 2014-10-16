package sched

import (
	"crypto/sha1"
	"fmt"
	"time"

	"github.com/StackExchange/bosun/expr"
)

type Silence struct {
	Start, End time.Time
	Alert      expr.AlertKey
}

func (s *Silence) Silenced(now time.Time, alert expr.AlertKey) bool {
	if now.Before(s.Start) || now.After(s.End) {
		return false
	}
	res := s.Matches(alert)
	fmt.Println(alert, res)
	return res
}

func (s *Silence) Matches(alert expr.AlertKey) bool {
	if s.Alert != "" && s.Alert != alert {
		return false
	}
	tags := alert.Group()
	for k, pattern := range s.Alert.Group() {
		tagv, ok := tags[k]
		if !ok {
			return false
		}
		matched, _ := Match(pattern, tagv)
		if !matched {
			return false
		}
	}
	return true
}

func (s Silence) ID() string {
	h := sha1.New()
	fmt.Fprintf(h, "%s|%s|%s", s.Start, s.End, s.Alert)
	return fmt.Sprintf("%x", h.Sum(nil))
}

// Silenced returns all currently silenced AlertKeys and the time they will be
// unsilenced.
func (s *Schedule) Silenced() map[expr.AlertKey]time.Time {
	aks := make(map[expr.AlertKey]time.Time)
	now := time.Now()
	s.Lock()
	for ak := range s.status {
		for _, si := range s.Silence {
			if si.Silenced(now, ak) {
				if aks[ak].Before(si.End) {
					aks[ak] = si.End
				}
			}
		}
	}
	s.Unlock()
	return aks
}

func (s *Schedule) AddSilence(start, end time.Time, alert, tagList string, confirm bool, edit string) (map[expr.AlertKey]bool, error) {
	if start.IsZero() || end.IsZero() {
		return nil, fmt.Errorf("both start and end must be specified")
	}
	if start.After(end) {
		return nil, fmt.Errorf("start time must be before end time")
	}
	if time.Since(end) > 0 {
		return nil, fmt.Errorf("end time must be in the future")
	}
	if alert == "" && tagList == "" {
		return nil, fmt.Errorf("must specify either alert or tags")
	}
	ak, err := expr.ParseAlertKey(alert + "{" + tagList + "}")
	if err != nil {
		return nil, err
	}
	si := &Silence{
		Start: start,
		End:   end,
		Alert: ak,
	}
	s.Lock()
	defer s.Unlock()
	if confirm {
		delete(s.Silence, edit)
		s.Silence[si.ID()] = si
		s.Save()
		return nil, nil
	}
	aks := make(map[expr.AlertKey]bool)
	for ak := range s.status {
		if si.Matches(ak) {
			aks[ak] = s.status[ak].IsActive()
		}
	}
	return aks, nil
}

func (s *Schedule) ClearSilence(id string) error {
	s.Lock()
	delete(s.Silence, id)
	s.Unlock()
	s.Save()
	return nil
}
