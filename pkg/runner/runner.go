// SPDX-License-Identifier: GPL-2.0-or-later

package runner

import (
	"errors"
	"os"
	"os/signal"
	"syscall"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/redhat-partner-solutions/vse-sync-collection-tools/pkg/callbacks"
	"github.com/redhat-partner-solutions/vse-sync-collection-tools/pkg/clients"
	"github.com/redhat-partner-solutions/vse-sync-collection-tools/pkg/collectors"
	"github.com/redhat-partner-solutions/vse-sync-collection-tools/pkg/utils"
)

const (
	maxRunningPolls      = 3
	pollResultsQueueSize = 10
)

// getQuitChannel creates and returns a channel for notifying
// that a exit signal has been received
func getQuitChannel() chan os.Signal {
	// Allow ourselves to handle shut down gracefully
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	return quit
}

type CollectorRunner struct {
	endTime                time.Time
	quit                   chan os.Signal
	collectorQuitChannel   map[string]chan os.Signal
	pollResults            chan collectors.PollResult
	erroredPolls           chan collectors.PollResult
	collectorInstances     map[string]collectors.Collector
	collectorNames         []string
	runningCollectorsWG    utils.WaitGroupCount
	runningAnnouncersWG    utils.WaitGroupCount
	pollInterval           int
	devInfoAnnouceInterval int
	onlyAnnouncers         bool
}

func NewCollectorRunner(selectedCollectors []string) *CollectorRunner {
	return &CollectorRunner{
		collectorInstances:   make(map[string]collectors.Collector),
		collectorNames:       GetCollectorsToRun(selectedCollectors),
		quit:                 getQuitChannel(),
		pollResults:          make(chan collectors.PollResult, pollResultsQueueSize),
		erroredPolls:         make(chan collectors.PollResult, pollResultsQueueSize),
		collectorQuitChannel: make(map[string]chan os.Signal, 1),
		onlyAnnouncers:       false,
	}
}

// initialise will call theconstructor for each
// value in collector name, it will panic if a collector name is not known.
func (runner *CollectorRunner) initialise( //nolint:funlen // allow a slightly long function
	callback callbacks.Callback,
	ptpInterface string,
	ptpNodeName string,
	clientset *clients.Clientset,
	pollInterval int,
	requestedDuration time.Duration,
	devInfoAnnouceInterval int,
	logsOutputFile string,
	includeLogTimestamps bool,
	tempDir string,
	keepDebugFiles bool,
) {
	runner.pollInterval = pollInterval
	runner.endTime = time.Now().Add(requestedDuration)
	runner.devInfoAnnouceInterval = devInfoAnnouceInterval

	constructor := &collectors.CollectionConstructor{
		Callback:               callback,
		PTPInterface:           ptpInterface,
		PTPNodeName:            ptpNodeName,
		Clientset:              clientset,
		PollInterval:           pollInterval,
		DevInfoAnnouceInterval: devInfoAnnouceInterval,
		ErroredPolls:           runner.erroredPolls,
		LogsOutputFile:         logsOutputFile,
		IncludeLogTimestamps:   includeLogTimestamps,
		TempDir:                tempDir,
		KeepDebugFiles:         keepDebugFiles,
	}

	registry := collectors.GetRegistry()

	for _, collectorName := range runner.collectorNames {
		builderFunc, err := registry.GetBuilderFunc(collectorName)
		if err != nil {
			log.Error(err)
			continue
		}

		newCollector, err := builderFunc(constructor)
		var missingRequirements *utils.RequirementsNotMetError
		if errors.As(err, &missingRequirements) {
			// Requirements are missing so don't add the collector to collectorInstance
			// so that it doesn't get ran
			log.Warning(err.Error())
		} else {
			utils.IfErrorExitOrPanic(err)
			runner.collectorInstances[collectorName] = newCollector
			log.Debugf("Added collector %T, %v", newCollector, newCollector)
		}
	}
	log.Debugf("Collectors %v", runner.collectorInstances)
	runner.setOnlyAnnouncers()
}

func (runner *CollectorRunner) setOnlyAnnouncers() {
	onlyAnnouncers := true
	for _, collector := range runner.collectorInstances {
		if !collector.IsAnnouncer() {
			onlyAnnouncers = false
			break
		}
	}
	runner.onlyAnnouncers = onlyAnnouncers
}

func (runner *CollectorRunner) shouldKeepPolling(
	collector collectors.Collector,
) bool {
	if collector.IsAnnouncer() && !runner.onlyAnnouncers {
		return runner.runningCollectorsWG.GetCount() > 0
	} else {
		return time.Since(runner.endTime) <= 0
	}
}

func (runner *CollectorRunner) poller(
	collectorName string,
	collector collectors.Collector,
	quit chan os.Signal,
	wg *utils.WaitGroupCount,
) {
	defer wg.Done()
	var lastPoll time.Time
	pollInterval := collector.GetPollInterval()
	runningPolls := utils.WaitGroupCount{}
	log.Debugf("Collector with poll interval %fs", pollInterval.Seconds())
	for runner.shouldKeepPolling(collector) {
		// If pollResults were to block we do not want to keep spawning polls
		// so we shouldn't allow too many polls to be running simultaneously
		if runningPolls.GetCount() >= maxRunningPolls {
			runningPolls.Wait()
		}
		log.Debugf("Collector GoRoutine: %s", collectorName)
		select {
		case <-quit:
			log.Infof("Killed shutting down collector %s waiting for running polls to finish", collectorName)
			runningPolls.Wait()
			return
		default:
			pollInterval = collector.GetPollInterval()
			log.Debug(
				"Collector GoRoutine:",
				collectorName,
				lastPoll, pollInterval,
				lastPoll.IsZero(),
				time.Since(lastPoll),
				time.Since(lastPoll) > pollInterval,
				lastPoll.IsZero() || time.Since(lastPoll) > pollInterval,
			)
			if lastPoll.IsZero() || time.Since(lastPoll) > pollInterval {
				lastPoll = time.Now()
				log.Debugf("poll %s", collectorName)
				runningPolls.Add(1)
				go collector.Poll(runner.pollResults, &runningPolls)
			}
			time.Sleep(time.Microsecond)
		}
	}
	runningPolls.Wait()
	log.Debugf("Collector finished %s", collectorName)
}

// start configures all collectors to start collecting all their data keys
func (runner *CollectorRunner) start() {
	for collectorName, collector := range runner.collectorInstances {
		log.Debugf("start collector %v", collector)
		err := collector.Start()
		utils.IfErrorExitOrPanic(err)

		log.Debugf("Spawning  collector: %v", collector)
		collectorName := collectorName
		collector := collector
		quit := make(chan os.Signal, 1)
		if collector.IsAnnouncer() {
			runner.collectorQuitChannel[collectorName] = quit
			runner.runningAnnouncersWG.Add(1)
			go runner.poller(collectorName, collector, quit, &runner.runningAnnouncersWG)
		} else {
			runner.collectorQuitChannel[collectorName] = quit
			runner.runningCollectorsWG.Add(1)
			go runner.poller(collectorName, collector, quit, &runner.runningCollectorsWG)
		}
	}
}

// cleanup calls cleanup on each collector
func (runner *CollectorRunner) cleanUpAll() {
	for collectorName, collector := range runner.collectorInstances {
		log.Debugf("cleanup %s", collectorName)
		err := collector.CleanUp()
		utils.IfErrorExitOrPanic(err)
	}
}

// Run manages set of collectors.
// It first initialises them,
// then polls them on the correct cadence and
// finally cleans up the collectors when exiting
func (runner *CollectorRunner) Run( //nolint:funlen // allow a slightly long function
	kubeConfig string,
	outputFile string,
	nodeName string,
	requestedDuration time.Duration,
	pollInterval int,
	devInfoAnnouceInterval int,
	ptpInterface string,
	useAnalyserJSON bool,
	logsOutputFile string,
	includeLogTimestamps bool,
	tempDir string,
	keepDebugFiles bool,
) {
	clientset, err := clients.GetClientset(kubeConfig)
	utils.IfErrorExitOrPanic(err)

	outputFormat := callbacks.Raw
	if useAnalyserJSON {
		outputFormat = callbacks.AnalyserJSON
	}

	callback, err := callbacks.SetupCallback(outputFile, outputFormat)
	utils.IfErrorExitOrPanic(err)
	runner.initialise(
		callback,
		ptpInterface,
		nodeName,
		clientset,
		pollInterval,
		requestedDuration,
		devInfoAnnouceInterval,
		logsOutputFile,
		includeLogTimestamps,
		tempDir,
		keepDebugFiles,
	)
	runner.start()

	// Use wg count to know if any collectors are running.
	for (runner.runningCollectorsWG.GetCount() + runner.runningAnnouncersWG.GetCount()) > 0 {
		log.Debugf("Main Loop ")
		select {
		case sig := <-runner.quit:
			log.Info("Killed shutting down")
			// Forward signal to collector QuitChannels
			for collectorName, quit := range runner.collectorQuitChannel {
				log.Infof("Killed shutting down: %s", collectorName)
				quit <- sig
			}
			runner.runningCollectorsWG.Wait()
			runner.runningAnnouncersWG.Wait()
		case pollRes := <-runner.pollResults:
			log.Infof("Received %v", pollRes)
			if len(pollRes.Errors) > 0 {
				log.Warnf("Poll %s had issues: %v. Will retry next poll", pollRes.CollectorName, pollRes.Errors)
				// If erroredPolls blocks it could cause pollResults to fill and
				// block the execution of the collectors.
				runner.erroredPolls <- pollRes
			}
		default:
			log.Debug("Sleeping main func")
			time.Sleep(time.Millisecond)
		}
	}
	log.Info("Doing Cleanup")
	runner.cleanUpAll()
	err = callback.CleanUp()
	utils.IfErrorExitOrPanic(err)
}
