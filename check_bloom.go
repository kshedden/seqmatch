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
	"strconv"
	"strings"
)

const (
	// File containing sequence reads we are matching (source of
	// matching); use the most upstream file that is practical to
	// use.
	sourcefile string = "PRT_NOV_15_02.txt.gz"

	// Candidate matching sequences
	matchfile string = "refined_matches.txt.gz"

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
	checks []matchinfo

	// A log
	logger *log.Logger
)

type matchinfo struct {
	target int
	pos    int
	weight int
	seq    string
}

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
		if rand.Float32() < 0.0001 {
			var mi matchinfo
			mi.target, err = strconv.Atoi(fields[0])
			if err != nil {
				panic(err)
			}
			mi.pos, err = strconv.Atoi(fields[1])
			if err != nil {
				panic(err)
			}
			mi.weight, err = strconv.Atoi(fields[2])
			if err != nil {
				panic(err)
			}
			mi.seq = fields[3]
			checks = append(checks, mi)
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
		seq := scanner.Text()
		if len(seq) < sw {
			continue
		}
		for j, ck := range checks {
			if !match[j] {
				if ck.seq[0:sw] == seq[0:sw] {
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
