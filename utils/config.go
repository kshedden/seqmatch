package utils

import (
	"encoding/json"
	"os"
)

type Config struct {

	// The name of the fastq file containing the reads.
	ReadFileName string

	// The name of the file containing the genes.
	GeneFileName string

	// Leveldb mapping gene integer id to full identifier.
	GeneIdDB

	// The left end point of each window.
	Windows []int

	// The width of each window.
	WindowWidth int

	// The size of the Bloom filter in bits.
	BloomSize uint64

	// The number of hash functions to use in the Bloom filter.
	NumHash int

	// The allowed proportion of mismatching values outside the
	// exact-match window.
	PMiss float64

	// The exact-match window must have this many distinct
	// dinucleotides.
	MinDinuc int

	// Skip all reads shorter than this length.
	MinReadLength int
}

func ReadConfig(filename string) *Config {
	fid, err := os.Open(filename)
	if err != nil {
		panic(err)
	}
	defer fid.Close()
	dec := json.NewDecoder(fid)
	config := new(Config)
	err = dec.Decode(config)
	if err != nil {
		panic(err)
	}
	return config
}
