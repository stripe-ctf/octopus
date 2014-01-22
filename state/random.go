package state

import (
	"bytes"
	"crypto/sha1"
	"encoding/binary"
	"fmt"
	"github.com/stripe-ctf/octopus/log"
	"math/rand"
)

func NewRand(name string) *rand.Rand {
	source := fmt.Sprintf("%s-%d", name, Seed())
	image := sha1.New().Sum([]byte(source))
	b := bytes.NewReader(image)
	derived, err := binary.ReadVarint(b)
	if err != nil {
		log.Fatalf("Bug in seed derivation: %s", err)
	}
	return rand.New(rand.NewSource(derived))
}

var alphabet = "abcdefghijkmnpqrstuvwxyz01234567890"

// There are lots of ways of doing this faster, but this is good
// enough.
func RandomString(rng *rand.Rand, size int) string {
	out := make([]byte, size)
	for i := 0; i < size; i++ {
		out[i] = alphabet[rng.Intn(size)]
	}
	return string(out)
}
