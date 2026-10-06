package compressor

import (
	"math"
	"math/bits"
)

// GorillaCompressor implements floating-point XOR and delta-of-delta time series compression
type GorillaCompressor struct {
	lastTimestamp int64
	lastDelta     int64
	lastValBits   uint64
	storedLeading uint8
	storedTrailing uint8
	first         bool
}

func NewGorillaCompressor() *GorillaCompressor {
	return &GorillaCompressor{
		first:          true,
		storedLeading:  math.MaxUint8,
		storedTrailing: 0,
	}
}

// CompressPair compresses a single timestamp and float64 sensor reading
func (g *GorillaCompressor) CompressPair(timestamp int64, value float64) (compressedBits int, xorVal uint64) {
	valBits := math.Float64bits(value)

	if g.first {
		g.lastTimestamp = timestamp
		g.lastDelta = 0
		g.lastValBits = valBits
		g.first = false
		return 64 + 64, 0 // Initial raw storage
	}

	// 1. Delta-of-Delta for timestamp
	newDelta := timestamp - g.lastTimestamp
	dod := newDelta - g.lastDelta
	g.lastDelta = newDelta
	g.lastTimestamp = timestamp

	var timeBits int
	if dod == 0 {
		timeBits = 1 // 1 bit '0'
	} else if dod >= -63 && dod <= 64 {
		timeBits = 2 + 7 // '10' + 7 bits
	} else if dod >= -255 && dod <= 256 {
		timeBits = 3 + 9 // '110' + 9 bits
	} else {
		timeBits = 4 + 32 // '1111' + 32 bits
	}

	// 2. XOR compression for float value
	xor := valBits ^ g.lastValBits
	g.lastValBits = valBits

	var valBitsCount int
	if xor == 0 {
		valBitsCount = 1 // 1 bit '0'
	} else {
		leading := uint8(bits.LeadingZeros64(xor))
		trailing := uint8(bits.TrailingZeros64(xor))

		if g.storedLeading != math.MaxUint8 && leading >= g.storedLeading && trailing >= g.storedTrailing {
			// Reuse leading/trailing bounds
			valBitsCount = 2 + int(64-g.storedLeading-g.storedTrailing)
		} else {
			// Write new bounds
			g.storedLeading = leading
			g.storedTrailing = trailing
			valBitsCount = 2 + 5 + 6 + int(64-leading-trailing)
		}
	}

	return timeBits + valBitsCount, xor
}
