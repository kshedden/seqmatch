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

	// Gene ids
	GeneIdFileName string

	// The left end point of each window.
	Windows []int

	// The width of each window.
	WindowWidth int

	// The size of the Bloom filter in bits.
	BloomSize uint64

	// The number of hash functions to use in the Bloom filter.
	NumHash int

	// The minimum allowed proportion matching values.
	PMatch float64

	// The exact-match window must have this many distinct
	// dinucleotides.
	MinDinuc int

	// Use this location to place temporary files.  If blank or
	// missing, a temporary file name is generated.
	TempDir string

	// Skip all reads shorter than this length.
	MinReadLength int

	// Return at most this many matches for each read (defaults to 1).
	MaxMatches int
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

	if config.MaxMatches == 0 {
		config.MaxMatches = 1
	}

	return config
}
