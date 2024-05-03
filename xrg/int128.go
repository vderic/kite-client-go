package xrg

import (
	"math/big"
	//"github.com/shabbyrobe/go-num"
)

const (
	LO      = 0
	HI      = 1
	signBit = 0x8000000000000000
)

var (
	maxBigU128, _ = new(big.Int).SetString("340282366920938463463374607431768211455", 10)
	big1          = new(big.Int).SetInt64(1)
)

type I128 struct {
	array []uint64
}

func (i I128) AsBigInt() (b *big.Int) {
	b = new(big.Int)
	neg := i.array[HI]&signBit != 0
	if i.array[HI] > 0 {
		b.SetUint64(i.array[HI])
		b.Lsh(b, 64)
	}
	var lo big.Int
	lo.SetUint64(i.array[LO])
	b.Add(b, &lo)

	if neg {
		b.Xor(b, maxBigU128).Add(b, big1).Neg(b)
	}
	return b
}

func (i I128) String() string {
	b := i.AsBigInt()
	return b.String()
}

func (i I128) GetHiLo() (hi uint64, lo uint64) {
	return i.array[HI], i.array[LO]
}
