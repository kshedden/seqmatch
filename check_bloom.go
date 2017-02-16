// Directly confirm that claimed matches in the results are actual
// matches.

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

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

const (
	// The source sequences
	sourcedb string = "PRT_NOV_15_02_0_80_seqdb"

	// The target sequences
	targetdb string = "target_seqdb"

	// Candidate matching sequences
	matchfile string = "PRT_NOV_15_02_0_80_rmatch.txt.gz"

	// Path to all data files
	dpath string = "/scratch/andjoh_fluxm/tealfurn/CSCAR"

	// Number of sequences to check
	ncheck int = 100
)

var (
	cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")

	// The sequences to check
	checks []matchinfo

	// The source database
	sdb *leveldb.DB

	// The target database
	tdb *leveldb.DB

	// A log
	logger *log.Logger
)

type matchinfo struct {
	target string
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
			mi.target = fields[0]
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

// checks compares the selected candidate matches to the actual reads
// to confirm that they are there
func checkReads() {

	var nmatch int
	for j, ck := range checks {

		_, err := sdb.Get([]byte(ck.seq), nil)
		if err != nil {
			panic(err)
		}

		ts, err := tdb.Get([]byte(ck.target), nil)
		if err != nil {
			panic(err)
		}
		tss := string(ts)[ck.pos : ck.pos+len(ck.seq)]

		if ck.seq != tss {
			print(j, "\n")
			print(ck.seq, "\n")
			print(tss, "\n\n")
		}

		nmatch++
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

	// Open the source database
	o := &opt.Options{
		ReadOnly: true,
	}
	p := path.Join(dpath, sourcedb)
	var err error
	sdb, err = leveldb.OpenFile(p, o)
	if err != nil {
		panic(err)
	}
	defer sdb.Close()

	// Open the target database
	p = path.Join(dpath, targetdb)
	tdb, err = leveldb.OpenFile(p, o)
	if err != nil {
		panic(err)
	}
	defer tdb.Close()

	setupLogger()
	readChecks()
	checkReads()
}
