// Identify possible matches of a set of source sequences into a set
// of target sequences.  The source sequences must all have the same
// length.  The target sequences may be of variable lengths.
//
// The result may contain false positives, but will not have any false
// negatives.

package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"path"
	"runtime/pprof"
	"strconv"
	"strings"

	"github.com/chmduquesne/rollinghash"
	"github.com/chmduquesne/rollinghash/buzhash"
	"github.com/golang-collections/go-datastructures/bitarray"
	"github.com/golang/snappy"
)

const (
	// File containing genes we are matching into (target of matching)
	targetfile string = "ALL_ABFVV_Genes_Derep_tr.txt.sz"

	// Path to all data files
	dpath string = "/scratch/andjoh_fluxm/tealfurn/CSCAR"

	// Some big constants for convenience
	mil int = 1000000
	bil int = 1000 * mil

	// Bloom filter size in bits
	bsize uint64 = 4 * uint64(bil)

	// Number of hashes to use in the Bloom filter
	nhash int = 30

	// Number of goroutines
	concurrency int = 100
)

var (
	cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")

	// A log
	logger *log.Logger

	// File containing sequence reads we are matching (source of matching)
	sourcefile string

	// The array that backs the Bloom filter
	smp bitarray.BitArray

	// Tables to produce independent running hashes
	tables [][256]uint32

	// The hash is computed from this subinterval
	hp1, hp2 int

	// The length of the hash window
	hlen int
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
	snr := snappy.NewReader(fid)

	scanner := bufio.NewScanner(snr)

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
		seq := toks[0]

		for _, ha := range hashes {
			ha.Reset()
			ha.Write([]byte(seq))
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
	mseq  string
	left  string
	right string
	tnum  int
	pos   uint32
}

func search() {

	fid, err := os.Open(path.Join(dpath, targetfile))
	if err != nil {
		panic(err)
	}
	defer fid.Close()
	snr := snappy.NewReader(fid)

	// Target file contains some very long lines
	scanner := bufio.NewScanner(snr)
	sbuf := make([]byte, 1024*1024)
	scanner.Buffer(sbuf, 1024*1024)

	hitchan := make(chan rec)
	limit := make(chan bool, concurrency)

	outname := strings.Replace(sourcefile, "_sorted.txt.sz", "_bmatch.txt.sz", -1)
	out, err := os.Create(path.Join(dpath, outname))
	if err != nil {
		panic(err)
	}
	defer out.Close()
	wtr := snappy.NewBufferedWriter(out)
	defer wtr.Close()

	// Retrieve the results and write to disk
	go func() {

		lw := 150
		bb := bytes.Repeat([]byte(" "), 150)
		bb[lw-1] = byte('\n')

		for r := range hitchan {
			n1, err1 := wtr.Write([]byte(fmt.Sprintf("%s\t", r.mseq)))
			n2, err2 := wtr.Write([]byte(fmt.Sprintf("%s\t", r.left)))
			n3, err3 := wtr.Write([]byte(fmt.Sprintf("%s\t", r.right)))
			n4, err4 := wtr.Write([]byte(fmt.Sprintf("%d\t", r.tnum)))
			n5, err5 := wtr.Write([]byte(fmt.Sprintf("%d", r.pos)))

			if err1 != nil || err2 != nil || err3 != nil || err4 != nil || err5 != nil {
				panic("writing error")
			}

			n := n1 + n2 + n3 + n4 + n5
			if n > lw {
				panic("output line is too long")
			}

			_, err := wtr.Write(bb[n:lw])
			if err != nil {
				panic(err)
			}
		}
	}()

	for i := 0; scanner.Scan(); i++ {

		if i%mil == 0 {
			logger.Printf("%d\n", i)
		}

		line := scanner.Text()
		toks := strings.Split(line, "\t")
		tname := toks[1]
		seq := toks[0]

		if len(seq) < hlen {
			continue
		}

		limit <- true
		go func(seq []byte, tname string) {

			// Count A and T
			var na, nt int
			for _, x := range seq[0:hlen] {
				switch x {
				case 'A':
					na++
				case 'T':
					nt++
				}
			}

			// Initialize the hashes
			hashes := make([]rollinghash.Hash32, nhash)
			for j, _ := range hashes {
				hashes[j] = buzhash.NewFromByteArray(tables[j])
				hashes[j].Write(seq[0:hlen])
			}

			// Check if the initial window is a match
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
			if g && hp1 == 0 && na < hlen-5 && nt < hlen-5 {
				jz := 100 - hp2
				if jz > len(seq) {
					jz = len(seq)
				}
				hitchan <- rec{
					mseq:  string(seq[0:hlen]),
					left:  "",
					right: string(seq[hlen:jz]),
					tnum:  i,
					pos:   0,
				}
			}

			// Check the rest of the windows
			for j := hlen; j < len(seq); j++ {

				// Update the A/T counts
				switch seq[j] {
				case 'T':
					nt++
				case 'A':
					na++
				}
				switch seq[j-hlen] {
				case 'T':
					nt--
				case 'A':
					na--
				}

				// Check for a match
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

				// Process a match
				// TODO constants should be configurable
				if g && j >= hp2-1 && na < hlen-5 && nt < hlen-5 {
					// Matching sequence is jx:jy
					jx := j - hlen + 1
					jy := j + 1

					// Left tail is jw:jx
					jw := jx - hp1
					if jw < 0 {
						jw = 0
					}

					// Right tail is jy:jz
					jz := jy + 100 - hp2
					if jz > len(seq) {
						jz = len(seq)
					}

					hitchan <- rec{
						mseq:  string(seq[jx:jy]),
						left:  string(seq[jw:jx]),
						right: string(seq[jy:jz]),
						tnum:  i,
						pos:   uint32(j - hlen + 1),
					}
				}
			}
			<-limit
		}([]byte(seq), tname)
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
	logname := strings.Replace(sourcefile, ".txt.sz", "_bmatch.log", -1)
	logfid, err := os.Create(logname)
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

	if len(os.Args) != 4 {
		panic("wrong number of arguments")
	}
	sourcefile = os.Args[1]
	var err error
	hp1, err = strconv.Atoi(os.Args[2])
	if err != nil {
		panic(err)
	}
	hp2, err = strconv.Atoi(os.Args[3])
	if err != nil {
		panic(err)
	}
	hlen = hp2 - hp1

	setupLogger()
	genTables()

	smp = bitarray.NewBitArray(bsize)

	buildBloom()
	search()
}
