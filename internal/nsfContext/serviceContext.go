package nsfContext

import (
	"encoding/json"
	"fmt"
	"nsf/internal/db"
	"nsf/logger"
	"sync"
)

// serviceRecord is the value stored in the DB for every registered service.
// The key is the service name, so one service name maps to exactly one NF.
type serviceRecord struct {
	NfName     string `json:"nf_name"`
	SbiAddress string `json:"sbi_address"`
	SbiPort    int    `json:"sbi_port"`
}

type serviceContextIE struct {
	db.DbIf

	*logger.NsfLogger
}

type serviceContext struct {
	db.DbIf

	// mu makes the multi-step "Exist -> Load -> Remove" sequences atomic,
	// because gin serves requests concurrently.
	mu sync.RWMutex

	*logger.NsfLogger
}

func newServiceContext(serviceContextIE *serviceContextIE) *serviceContext {
	return &serviceContext{
		DbIf: serviceContextIE.DbIf,

		NsfLogger: serviceContextIE.NsfLogger,
	}
}

func (s *serviceContext) release() {
	s.NsfLogger.SvcLog.Infoln("Release serviceContext...")

	// All state lives in the DB, and the DB is closed by dbContext.release(),
	// so there is nothing to release (or to close twice) here.

	s.NsfLogger.SvcLog.Infoln("serviceContext released")
}

func (s *serviceContext) registerServices(nfName, sbiAddr string, sbiPort int, services []string) error {
	s.NsfLogger.SvcLog.Tracef("Registering %s services", nfName)

	if len(services) == 0 {
		return nil
	}

	value, err := json.Marshal(serviceRecord{
		NfName:     nfName,
		SbiAddress: sbiAddr,
		SbiPort:    sbiPort,
	})
	if err != nil {
		return fmt.Errorf("failed to encode services of %s: %w", nfName, err)
	}

	data := make(map[string]string, len(services))
	for _, service := range services {
		data[service] = string(value)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.DbIf.SaveAll(data); err != nil {
		return fmt.Errorf("failed to save services of %s: %w", nfName, err)
	}
	return nil
}

func (s *serviceContext) deregisterServices(nfName string, services []string) error {
	s.NsfLogger.SvcLog.Tracef("De-registering %s services", nfName)

	s.mu.Lock()
	defer s.mu.Unlock()

	for _, service := range services {
		record, found, err := s.loadRecord(service)
		if err != nil {
			return err
		}
		if !found {
			s.NsfLogger.SvcLog.Infof("Service %s is not registered, skip", service)
			continue
		}
		if record.NfName != nfName {
			s.NsfLogger.SvcLog.Infof("Service %s belongs to %s, not %s, skip", service, record.NfName, nfName)
			continue
		}

		if err := s.DbIf.Remove(service); err != nil {
			return fmt.Errorf("failed to remove service %s: %w", service, err)
		}
	}
	return nil
}

func (s *serviceContext) discoverService(service string) (string, string, int, error) {
	s.NsfLogger.SvcLog.Tracef("Discovering service %s", service)

	s.mu.RLock()
	defer s.mu.RUnlock()

	record, found, err := s.loadRecord(service)
	if err != nil {
		return "", "", 0, err
	}
	if !found {
		return "", "", 0, fmt.Errorf("service %s not found", service)
	}

	return record.NfName, record.SbiAddress, record.SbiPort, nil
}

// loadRecord must be called with s.mu held. It checks Exist first because
// bboltDb.Load returns an empty string (no error) for a missing key.
func (s *serviceContext) loadRecord(service string) (*serviceRecord, bool, error) {
	exist, err := s.DbIf.Exist(service)
	if err != nil {
		return nil, false, fmt.Errorf("failed to check service %s: %w", service, err)
	}
	if !exist {
		return nil, false, nil
	}

	value, err := s.DbIf.Load(service)
	if err != nil {
		return nil, false, fmt.Errorf("failed to load service %s: %w", service, err)
	}

	var record serviceRecord
	if err := json.Unmarshal([]byte(value), &record); err != nil {
		return nil, false, fmt.Errorf("failed to decode service %s: %w", service, err)
	}
	return &record, true, nil
}
