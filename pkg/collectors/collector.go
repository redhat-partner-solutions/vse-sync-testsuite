// SPDX-License-Identifier: GPL-2.0-or-later

package collectors

import (
	"sync"
	"time"

	"github.com/redhat-partner-solutions/vse-sync-collection-tools/pkg/callbacks"
	"github.com/redhat-partner-solutions/vse-sync-collection-tools/pkg/clients"
	"github.com/redhat-partner-solutions/vse-sync-collection-tools/pkg/utils"
)

type Collector interface {
	Start() error                                // Setups any internal state required for collection to happen
	Poll(chan PollResult, *utils.WaitGroupCount) // Poll for collectables
	CleanUp() error                              // Stops the collector and cleans up any internal state. It should result in a state that can be started again
	GetPollInterval() time.Duration              // Returns the collectors polling interval
	IsAnnouncer() bool
	ScalePollInterval(float64)
	ResetPollInterval()
}

// A union of all values required to be passed into all constructions
type CollectionConstructor struct {
	Callback               callbacks.Callback
	Clientset              *clients.Clientset
	ErroredPolls           chan PollResult
	PTPInterface           string
	PTPNodeName            string
	Msg                    string
	LogsOutputFile         string
	TempDir                string
	PollInterval           int
	DevInfoAnnouceInterval int
	IncludeLogTimestamps   bool
	KeepDebugFiles         bool
}

type PollResult struct {
	CollectorName string
	Errors        []error
}

type baseCollector struct {
	callback     callbacks.Callback
	pollInterval *LockedInterval
	isAnnouncer  bool
	running      bool
}

func (base *baseCollector) GetPollInterval() time.Duration {
	return base.pollInterval.interval()
}

func (base *baseCollector) ScalePollInterval(factor float64) {
	base.pollInterval.scale(factor)
}

func (base *baseCollector) ResetPollInterval() {
	base.pollInterval.reset()
}

func (base *baseCollector) IsAnnouncer() bool {
	return base.isAnnouncer
}

func (base *baseCollector) Start() error {
	base.running = true
	return nil
}

func (base *baseCollector) CleanUp() error {
	base.running = false
	return nil
}

func newBaseCollector(
	pollInterval int,
	isAnnouncer bool,
	callback callbacks.Callback,
) *baseCollector {
	return &baseCollector{
		callback:     callback,
		isAnnouncer:  isAnnouncer,
		running:      false,
		pollInterval: NewLockedInterval(pollInterval),
	}
}

type LockedInterval struct {
	current time.Duration
	base    time.Duration
	lock    sync.RWMutex
}

func (li *LockedInterval) interval() time.Duration {
	li.lock.RLock()
	defer li.lock.RUnlock()
	return li.current
}

func (li *LockedInterval) scale(factor float64) {
	li.lock.Lock()
	li.current = time.Duration(factor * li.current.Seconds() * float64(time.Second))
	li.lock.Unlock()
}

func (li *LockedInterval) reset() {
	li.lock.Lock()
	li.current = li.base
	li.lock.Unlock()
}

func NewLockedInterval(seconds int) *LockedInterval {
	return &LockedInterval{
		current: time.Duration(seconds) * time.Second,
		base:    time.Duration(seconds) * time.Second,
	}
}
