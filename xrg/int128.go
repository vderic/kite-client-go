package xrg

import (
	"math/big"
)

const (
	signBit = 0x8000000000000000
)

var (
	maxBigU128, _ = new(big.Int).SetString("340282366920938463463374607431768211455", 10)
	big1          = new(big.Int).SetInt64(1)
)

/*
    0 - lo
    1 - hi
type I128 struct {
    array []uint64
)
*/

func I128ToBigInt(i []uint64) (b *big.Int) {
	b = new(big.Int)
	neg := i[1]&signBit != 0
	if i[1] > 0 {
		b.SetUint64(i[1])
		b.Lsh(b, 64)
	}
	var lo big.Int
	lo.SetUint64(i[0])
	b.Add(b, &lo)

	if neg {
		b.Xor(b, maxBigU128).Add(b, big1).Neg(b)
	}
	return b
}
