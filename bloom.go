package main

import (
	"bufio"
	"compress/gzip"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"path"
	"runtime/pprof"
	"strings"

	"github.com/chmduquesne/rollinghash"
	"github.com/chmduquesne/rollinghash/buzhash"
	"github.com/golang-collections/go-datastructures/bitarray"
)

const (
	// File containing sequence reads we are matching (source of matching)
	sourcefile string = "PRT_NOV_15_02_sorted.txt.gz"

	// File containing genes we are matching into (target of matching)
	targetfile string = "ALL_ABFVV_Genes_Derep.txt.gz"

	// Path to all data files
	dpath string = "/scratch/andjoh_fluxm/tealfurn/CSCAR"

	// Some big constants for convenience
	mil int = 1000000
	bil int = 1000 * mil

	// Bloom filter size in bits
	bsize uint64 = 4 * uint64(bil)

	// Search for matches based on this sequence length
	sw int = 80

	// Number of hashes to use in the Bloom filter
	nhash int = 30

	concurrency int = 100
)

var (
	cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")

	// A log
	logger *log.Logger

	// The array that backs the Bloom filter
	smp bitarray.BitArray

	// Tables to produce independent running hashes
	tables [][256]uint32
)

func genTables() {
	tables = make([][256]uint32, nhash)
	for j := 0; j < nhash; j++ {
		mp := make(map[uint32]bool)
		for i := 0; i < 256; i++ {
			for {
				x := uint32(rand.Int63())
				if !mp[x] {
					tables[j][i] = x
					mp[x] = true
					break
				}
			}
		}
	}
}

func buildBloom() {

	fid, err := os.Open(path.Join(dpath, sourcefile))
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

	hashes := make([]rollinghash.Hash32, nhash)
	for j, _ := range hashes {
		hashes[j] = buzhash.NewFromByteArray(tables[j])
	}

	for j := 0; scanner.Scan(); j++ {

		if j%mil == 0 {
			logger.Printf("%d\n", j)
		}

		line := scanner.Text()
		toks := strings.Fields(line)
		seq := toks[1]

		if len(seq) < sw {
			continue
		}

		for _, ha := range hashes {
			ha.Reset()
			ha.Write([]byte(seq[0:sw]))
			x := uint64(ha.Sum32()) % bsize
			err := smp.SetBit(x)
			if err != nil {
				panic(err)
			}
		}
	}

	if err := scanner.Err(); err != nil {
		panic(err)
	}

	logger.Printf("Done constructing filter")
}

type rec struct {
	seq    string
	target uint32
	pos    uint32
}

func check() {

	fid, err := os.Open(path.Join(dpath, targetfile))
	if err != nil {
		panic(err)
	}
	defer fid.Close()
	gzr, err := gzip.NewReader(fid)
	if err != nil {
		panic(err)
	}
	defer gzr.Close()

	// Target file contains some very long lines
	scanner := bufio.NewScanner(gzr)
	sbuf := make([]byte, 1024*1024)
	scanner.Buffer(sbuf, 1024*1024)

	hitchan := make(chan rec)
	limit := make(chan bool, concurrency)

	out, err := os.Create(path.Join(dpath, "bloom_matches.txt.gz"))
	if err != nil {
		panic(err)
	}
	defer out.Close()
	wtr := gzip.NewWriter(out)
	defer wtr.Close()

	// Retrieve the results and write to disk
	go func() {
		for r := range hitchan {
			seq := r.seq
			if len(seq) > 120 {
				seq = seq[0:120]
			}
			wtr.Write([]byte(fmt.Sprintf("%s\t", seq)))
			wtr.Write([]byte(fmt.Sprintf("%d\t", r.target)))
			wtr.Write([]byte(fmt.Sprintf("%d\n", r.pos)))
		}
	}()

	for i := 0; scanner.Scan(); i++ {

		if i%mil == 0 {
			logger.Printf("%d\n", i)
		}

		line := scanner.Text()
		toks := strings.Split(line, "\t")
		seq := toks[1]

		if len(seq) < sw {
			continue
		}

		limit <- true
		go func(seq []byte, i int) {
			hashes := make([]rollinghash.Hash32, nhash)
			for j, _ := range hashes {
				hashes[j] = buzhash.NewFromByteArray(tables[j])
				hashes[j].Write(seq[0:sw])
			}

			// Check the initial window
			g := true
			for _, ha := range hashes {
				x := uint64(ha.Sum32()) % bsize
				f, err := smp.GetBit(x)
				if err != nil {
					panic(err)
				}
				if !f {
					g = false
					break
				}
			}
			if g {
				m := 120
				if m > len(seq) {
					m = len(seq)
				}
				hitchan <- rec{seq: string(seq), target: uint32(i), pos: 0}
			}

			// Check the rest of the windows
			for j := sw; j < len(seq); j++ {
				g := true
				for _, ha := range hashes {
					ha.Roll(seq[j])
					if g {
						// Still a candidate, keep checking
						x := uint64(ha.Sum32()) % bsize
						f, err := smp.GetBit(x)
						if err != nil {
							panic(err)
						}
						g = g && f
					}
				}
				if g {
					// Match
					hitchan <- rec{seq: string(seq[j-sw+1 : length(seq)]), target: uint32(i), pos: uint32(j - sw + 1)}
				}
			}
			<-limit
		}([]byte(seq), i)
	}

	if err := scanner.Err(); err != nil {
		panic(err)
	}

	for k := 0; k < concurrency; k++ {
		limit <- true
	}
	close(hitchan)
}

func setupLogger() {
	logfid, err := os.Create("bloom.log")
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
	genTables()

	smp = bitarray.NewBitArray(bsize)

	buildBloom()
	check()
}
