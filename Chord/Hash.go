package DHT

import (
	"crypto/sha1"
	"math/big"
)

func GetHash(k string) *big.Int {
	h := sha1.New()
	h.Write([]byte(k))
	hRes := h.Sum(nil)
	var hash big.Int
	hash.SetBytes(hRes)
	return &hash
}
