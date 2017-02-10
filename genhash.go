// Generate a collection of minhash/maxhash values for all source and
// target sequences.  Only the tails of the distribution are saved.

package main

import (
	"bufio"
	"compress/gzip"
	"encoding/binary"
	"log"
	"math/rand"
	"os"
	"path"
	"strings"
	"sync"

	"github.com/chmduquesne/rollinghash/buzhash"
)

const (
	// File containing sequence reads we are matching (source of matching)
	sourcefile string = "PRT_NOV_15_02.txt.gz"

	// File containing genes we are matching into (target of matching)
	targetfile string = "ALL_ABFVV_Genes_Derep.txt.gz"

	// Path to all data files
	dpath string = "/scratch/andjoh_fluxm/tealfurn/CSCAR"

	// Number of hashes
	nhash int = 100

	// The kmer length of each hash
	hashlen int = 20

	// Limit concurrency to this amount
	concurrency int = 100

	// Only keep if the minhash/maxhash is beyond this threshold in magnitude
	minthresh uint32 = 4096
	maxthresh uint32 = 4294965000
)

var (
	// A log
	logger *log.Logger
)

func genTables() [][256]uint32 {

	tables := make([][256]uint32, nhash)
	for j := 0; j < nhash; j++ {

		mp := make(map[uint32]bool)
		for i := 0; i < hashlen; i++ {
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

	return tables
}

// Returns the minimum and maximum of the hash applied to the given sequence
func gethash(seq []byte, table [256]uint32) (uint32, uint32) {

	ha := buzhash.NewFromByteArray(table)

	ha.Write(seq[0:hashlen])

	min := ha.Sum32()
	max := min

	for j := hashlen; j < len(seq); j++ {

		ha.Roll(seq[j])
		u := ha.Sum32()

		if u < min {
			min = u
		}
		if u > max {
			max = u
		}
	}

	return min, max
}

type qrec struct {
	pos  uint32
	hash uint32
	ch   uint16
}

func genhash(tables [][256]uint32, infile string, wg *sync.WaitGroup) {

	var infname string
	var outdirname string
	if infile == sourcefile {
		outdirname = path.Join(dpath, "source_hashes")
		infname = path.Join(dpath, sourcefile)
	} else {
		outdirname = path.Join(dpath, "target_hashes")
		infname = path.Join(dpath, targetfile)
	}
	os.MkdirAll(outdirname, 0755)

	fid, err := os.Open(infname)
	if err != nil {
		panic(err)
	}
	defer fid.Close()
	gzr, err := gzip.NewReader(fid)
	if err != nil {
		panic(err)
	}
	defer gzr.Close()

	nhash := len(tables)

	// Set up a scanner to read long lines
	scanner := bufio.NewScanner(gzr)
	sbuf := make([]byte, 1024*1024)
	scanner.Buffer(sbuf, 1024*1024)

	// Create files to store the results.
	oname := path.Join(outdirname, "min.bin")
	outmin, err := os.Create(oname)
	if err != nil {
		panic(err)
	}
	defer outmin.Close()
	oname = path.Join(outdirname, "max.bin")
	outmax, err := os.Create(oname)
	if err != nil {
		panic(err)
	}
	defer outmax.Close()

	minchan := make(chan qrec)
	maxchan := make(chan qrec)
	limit := make(chan bool, concurrency)
	alldone := make(chan bool, 1)

	// Harvest the data and write to disk
	go func() {
		for {
			select {
			case r, ok := <-minchan:
				if !ok {
					minchan = nil
				} else {
					err = binary.Write(outmin, binary.LittleEndian, &r)
					if err != nil {
						panic(err)
					}
				}
			case r, ok := <-maxchan:
				if !ok {
					maxchan = nil
				} else {
					err = binary.Write(outmax, binary.LittleEndian, &r)
					if err != nil {
						panic(err)
					}
				}
			}

			if minchan == nil && maxchan == nil {
				alldone <- true
				break
			}
		}
	}()

	// Loop over lines of input file
	for lnum := 0; scanner.Scan(); lnum++ {

		if lnum%1000000 == 0 {
			logger.Printf("%s %d", infile, lnum)
		}

		// Read a line and check for errors.
		line := scanner.Text()
		if err := scanner.Err(); err != nil {
			panic(err)
		}

		toks := strings.Split(line, "\t")
		seq := toks[1]

		limit <- true
		go func(isq []uint8, lnum int) {
			for ci := 0; ci < nhash; ci++ {
				minhash, maxhash := gethash([]byte(seq), tables[ci])
				if minhash < minthresh {
					minchan <- qrec{uint32(lnum), minhash, uint16(ci)}
				} else if maxhash > maxthresh {
					maxchan <- qrec{uint32(lnum), maxhash, uint16(ci)}
				}
			}
			<-limit
		}([]byte(seq), lnum)
	}

	if err := scanner.Err(); err != nil {
		panic(err)
	}

	// Fill the buffer to make sure that all the sequences are processed.
	for k := 0; k < concurrency; k++ {
		limit <- true
	}
	close(minchan)
	close(maxchan)
	<-alldone

	wg.Done()
}

func setupLogger() {
	logfid, err := os.Create("genhash.log")
	if err != nil {
		panic(err)
	}
	logger = log.New(logfid, "", log.Lshortfile)
}

func main() {

	setupLogger()

	tables := genTables()

	var wg sync.WaitGroup

	wg.Add(2)
	go genhash(tables, sourcefile, &wg)
	go genhash(tables, targetfile, &wg)
	wg.Wait()
}
