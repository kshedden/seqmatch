package main

import (
	"bufio"
	"compress/gzip"
	"flag"
	"log"
	"math/rand"
	"os"
	"path"
	"runtime/pprof"
	"strings"
)

const (
	// File containing sequence reads we are matching (source of matching)
	sourcefile string = "PRT_NOV_15_02_sorted.txt.gz"

	// Candidate matching sequences
	matchfile string = "bloom_matches.txt.gz"

	// Path to all data files
	dpath string = "/scratch/andjoh_fluxm/tealfurn/CSCAR"

	// Number of sequences to check
	ncheck int = 100

	// Check the initial subsequence with this length.
	sw int = 80
)

var (
	cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")

	// The sequences to check
	checks []string

	// A log
	logger *log.Logger
)

// readChecks selects some candidate match sequences at random
func readChecks() {

	fid, err := os.Open(path.Join(dpath, matchfile))
	if err != nil {
		panic(err)
	}
	defer fid.Close()
	gzr, err := gzip.NewReader(fid)
	if err != nil {
		panic(err)
	}
	defer gzr.Close()
	scanner := bufio.NewScanner(gzr)

	for scanner.Scan() {
		line := scanner.Text()
		fields := strings.Fields(line)
		if rand.Float32() < 0.01 {
			checks = append(checks, fields[0][0:sw])
		}
		if len(checks) >= ncheck {
			break
		}
	}

	if err := scanner.Err(); err != nil {
		panic(err)
	}
}

// checks compares the selected candidate matches to the actual reads to confirm that they are there
func check() {

	fid, err := os.Open(path.Join(dpath, sourcefile))
	if err != nil {
		panic(err)
	}
	defer fid.Close()
	gzr, err := gzip.NewReader(fid)
	if err != nil {
		panic(err)
	}

	scanner := bufio.NewScanner(gzr)
	match := make([]bool, len(checks))
	nmatch := 0

	for j := 0; scanner.Scan(); j++ {
		line := scanner.Text()
		toks := strings.Fields(line)
		seq := toks[1]
		if len(seq) < sw {
			continue
		}
		for j, ck := range checks {
			if !match[j] {
				if seq[0:sw] == ck {
					match[j] = true
					nmatch++
				}
			}
		}
		if nmatch == ncheck {
			break
		}
	}

	if err := scanner.Err(); err != nil {
		panic(err)
	}

	logger.Printf("%d\n", nmatch)
}

func setupLogger() {
	logfid, err := os.Create("check_bloom.log")
	if err != nil {
		panic(err)
	}
	logger = log.New(logfid, "", log.Lshortfile)
}

func main() {

	flag.Parse()
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	setupLogger()
	readChecks()
	check()
}
