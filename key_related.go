package copeer

import (
	"encoding/hex"
	"math/rand"
	"fmt"
	"crypto/sha1"
)

const keySpaceDim = 160

const keyLenBytes = keySpaceDim / 8

type dhtKey [keyLenBytes]byte

func NewRandomDhtKey() (ret dhtKey) {
	for i := 0; i < keyLenBytes; i++ {
		ret[i] = uint8(rand.Intn(256))
	}
	return
}

func NewRandomDhtKeyHash(data []byte) (ret dhtKey) {
	data = append(data, byte(rand.Intn(256)))
	ret = sha1.Sum(data)
	return
}

func NewZeroDhtKey() (ret dhtKey) {
	for i := 0; i < keyLenBytes; i++ {
		ret[i] = uint8(0)
	}
	return
}

func (key dhtKey) xor(other dhtKey) (ret dhtKey) {
	for i := 0; i < keyLenBytes; i++ {
		ret[i] = key[i] ^ other[i]
	}
	return
}

func (key dhtKey) String1() string {
	return hex.EncodeToString(key[0:keyLenBytes]);
}

func (key dhtKey) String() string {
	var ret = fmt.Sprintf("%d", key[0])
	for i := 1; i < keyLenBytes; i++ {
		ret += "_" + fmt.Sprintf("%d", key[i])
	}
	return ret
}


func (key dhtKey) Equals(other dhtKey) bool {
	for i := 0; i < keyLenBytes; i++ {
		if key[i] != other[i] {
			return false
		}
	}
	return true
}

func (key dhtKey) Less(other dhtKey) bool {
	for i := 0; i < keyLenBytes; i++ {
		if key[i] != other[i] {
			return key[i] < other[i]
		}
	}
	return false
}