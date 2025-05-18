package main

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
)

type EquivocationAlert struct {
	LeaderID        string
	Index           int
	Term            int
	LocalHash       string
	RemoteHash      string
	LocalSignature  string
	RemoteSignature string
}

func NewEquivocationAlertIfApplicable(entry LogEntry, leaderId string, s *Server) (*EquivocationAlert, error) {
	// Check for same index and term but different content
	if entry.Index < len(s.log) {
		existing := s.log[entry.Index]
		if existing.Term == entry.Term && existing.HashEntry() != entry.HashEntry() {
			return &EquivocationAlert{
				LeaderID:   leaderId,
				Index:      entry.Index,
				Term:       entry.Term,
				LocalHash:  existing.HashEntry(),
				RemoteHash: entry.HashEntry(),
			}, nil
		}
	}
	return nil, nil
}

// NewEquivocationAlert cria uma instância de alerta a partir de entradas conflitantes
func NewEquivocationAlert(leaderID string, idx, term int, local, remote LogEntry) EquivocationAlert {
	return EquivocationAlert{
		LeaderID:        leaderID,
		Index:           idx,
		Term:            term,
		LocalHash:       hashEntry(local),
		RemoteHash:      hashEntry(remote),
		LocalSignature:  local.Signature,
		RemoteSignature: remote.Signature,
	}
}

// hashEntry computa o hash de conteúdo e termo de uma entrada
func hashEntry(e LogEntry) string {
	h := sha256.New()
	h.Write([]byte(e.Command))
	h.Write([]byte(string(e.Term)))
	return hex.EncodeToString(h.Sum(nil))
}

// ToRPCMessage converte o alerta em formato genérico para envio
func (a EquivocationAlert) ToRPCMessage() map[string]interface{} {
	return map[string]interface{}{
		"type":       "equivocation_alert",
		"leader":     a.LeaderID,
		"index":      a.Index,
		"term":       a.Term,
		"hashes":     []string{a.LocalHash, a.RemoteHash},
		"signatures": []string{a.LocalSignature, a.RemoteSignature},
	}
}

// ValidateAlert verifica consistência de hashes e presença de assinaturas
func ValidateAlert(a EquivocationAlert) error {
	// checa se hashes coincidem com assinaturas não alteradas
	if a.LocalHash == a.RemoteHash {
		return errors.New("nenhum conflito: hashes idênticos")
	}
	// aqui seria verificação de assinatura digital real
	if a.LocalSignature == "" || a.RemoteSignature == "" {
		return errors.New("assinatura ausente em ao menos uma versão")
	}
	return nil
}

// CountKey retorna chave única para map de contagem de alerts
func (a EquivocationAlert) CountKey() string {
	return a.LeaderID + ":" +
		string(a.Index) + ":" +
		string(a.Term)
}
