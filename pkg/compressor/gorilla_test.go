package compressor

import (
	"testing"
)

func TestGorillaCompressor_InitialSample(t *testing.T) {
	c := NewGorillaCompressor()
	bits, _ := c.CompressPair(1000, 25.4)
	if bits != 128 {
		t.Fatalf("Expected initial sample to require 128 bits, got %d", bits)
	}
}

func TestGorillaCompressor_ConstantStream(t *testing.T) {
	c := NewGorillaCompressor()
	// Step 1: initial
	c.CompressPair(1000, 50.0)

	// Step 2: Establish delta = 1000
	c.CompressPair(2000, 50.0)

	// Step 3: Same interval (delta-of-delta = 0) and same value (XOR = 0)
	bits, _ := c.CompressPair(3000, 50.0)

	// dod=0 -> 1 bit ('0')
	// xor=0 -> 1 bit ('0')
	// Total = 2 bits!
	if bits != 2 {
		t.Fatalf("Expected optimal constant stream compression of 2 bits, got %d", bits)
	}
}

func TestGorillaCompressor_DynamicStream(t *testing.T) {
	c := NewGorillaCompressor()
	c.CompressPair(1000, 20.0)

	totalBits := 128
	for i := 1; i <= 100; i++ {
		ts := int64(1000 + i*100)
		val := 20.0 + float64(i)*0.05
		bits, _ := c.CompressPair(ts, val)
		totalBits += bits
	}

	rawBits := 101 * 128
	compressionRatio := float64(rawBits) / float64(totalBits)

	if compressionRatio < 1.5 {
		t.Fatalf("Expected compression ratio > 1.5x, got %.2fx", compressionRatio)
	}
}

func BenchmarkGorillaCompressor(b *testing.B) {
	c := NewGorillaCompressor()
	c.CompressPair(1000, 25.0)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.CompressPair(int64(2000+i*10), 25.0+float64(i%10)*0.1)
	}
}
