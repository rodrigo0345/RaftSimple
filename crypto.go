package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"log"
	"math/big"
)

// KeyPair represents an ECDSA public/private key pair
type KeyPair struct {
	PrivateKey *ecdsa.PrivateKey
	PublicKey  *ecdsa.PublicKey
}

// Signature represents an ECDSA signature
type Signature struct {
	R *big.Int
	S *big.Int
}

// GenerateKeyPair creates a new ECDSA key pair
func GenerateKeyPair() (*KeyPair, error) {
	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, err
	}
	return &KeyPair{
		PrivateKey: privateKey,
		PublicKey:  &privateKey.PublicKey,
	}, nil
}

// SignMessage signs a message with the private key
func SignMessage(message []byte, privateKey *ecdsa.PrivateKey) (*Signature, error) {
	hash := sha256.Sum256(message)
	r, s, err := ecdsa.Sign(rand.Reader, privateKey, hash[:])
	if err != nil {
		return nil, err
	}
	return &Signature{R: r, S: s}, nil
}

// VerifySignature verifies a signature with the public key
func VerifySignature(message []byte, signature *Signature, publicKey *ecdsa.PublicKey) bool {
	hash := sha256.Sum256(message)
	return ecdsa.Verify(publicKey, hash[:], signature.R, signature.S)
}

// SignData signs arbitrary data (struct or map) and returns the signature
func SignData(data interface{}, privateKey *ecdsa.PrivateKey) (*Signature, error) {
	jsonData, err := json.Marshal(data)
	if err != nil {
		return nil, fmt.Errorf("error marshaling data for signing: %v", err)
	}
	return SignMessage(jsonData, privateKey)
}

// VerifyData verifies the signature of arbitrary data
func VerifyData(data interface{}, signature *Signature, publicKey *ecdsa.PublicKey) bool {
	jsonData, err := json.Marshal(data)
	if err != nil {
		log.Printf("Error marshaling data for verification: %v", err)
		return false
	}
	return VerifySignature(jsonData, signature, publicKey)
}

// SerializeSignature converts a signature to a map for JSON serialization
func SerializeSignature(sig *Signature) map[string]string {
	return map[string]string{
		"r": sig.R.String(),
		"s": sig.S.String(),
	}
}

// DeserializeSignature converts a serialized signature back to a Signature struct
func DeserializeSignature(sigMap map[string]string) (*Signature, error) {
	r := new(big.Int)
	s := new(big.Int)

	r, ok := r.SetString(sigMap["r"], 10)
	if !ok {
		return nil, fmt.Errorf("failed to parse R value")
	}

	s, ok = s.SetString(sigMap["s"], 10)
	if !ok {
		return nil, fmt.Errorf("failed to parse S value")
	}

	return &Signature{R: r, S: s}, nil
}

// SerializePublicKey converts a public key to a map for JSON serialization
func SerializePublicKey(publicKey *ecdsa.PublicKey) map[string]string {
	return map[string]string{
		"x": publicKey.X.String(),
		"y": publicKey.Y.String(),
	}
}

// DeserializePublicKey converts a serialized public key back to an ecdsa.PublicKey
func DeserializePublicKey(keyMap map[string]string) (*ecdsa.PublicKey, error) {
	x := new(big.Int)
	y := new(big.Int)

	x, ok := x.SetString(keyMap["x"], 10)
	if !ok {
		return nil, fmt.Errorf("failed to parse X value")
	}

	y, ok = y.SetString(keyMap["y"], 10)
	if !ok {
		return nil, fmt.Errorf("failed to parse Y value")
	}

	return &ecdsa.PublicKey{
		Curve: elliptic.P256(),
		X:     x,
		Y:     y,
	}, nil
}
