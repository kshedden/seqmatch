// Generate a collection of minhash/maxhash values for all source and
// target sequences.  Only the tails of the distribution are saved.

package main

import (
	"bufio"
	"compress/gzip"
	"encoding/binary"
	"fmt"
	"log"
	"math/rand"
	"os"
	"path"
	"strings"
	"sync"

	"github.com/OneOfOne/xxhash"
)

var (
	// File containing sequence reads we are matching (source of matching)
	sourcefile string = "PRT_NOV_15_02.txt.gz"

	// File containing genes we are matching into (target of matching)
	targetfile string = "ALL_ABFVV_Genes_Derep.txt.gz"

	// Path to all data files
	dpath string = "/scratch/andjoh_fluxm/tealfurn/CSCAR"

	// A log
	logger *log.Logger

	// Number of hashes
	nhash int = 100

	// The kmer length of each hash
	hashlen int = 20

	// Only keep if the minhash/maxhash is beyond this threshold in magnitude
	hashbase  uint64 = 2 << 32
	minthresh uint64
	maxthresh uint64
)

func genSalt() [][]byte {

	x := make([][]byte, nhash)
	for j := 0; j < nhash; j++ {
		x[j] = make([]byte, 16)
		for k := 0; k < 4; k++ {
			x[j][k] = byte(rand.Int() % 256)
		}
	}

	return x
}

// Returns the minimum and maximum of the hash applied to the given sequence
func gethash(isq []byte, salt []byte) (uint64, uint64) {

	var minhash, maxhash uint64
	ha := xxhash.New64()

	for j := 0; j <= len(isq)-hashlen; j++ {
		ha.Reset()
		ha.Write(salt)
		ha.Write(isq[j : j+hashlen])
		x := ha.Sum64() % hashbase
		if j == 0 || x < minhash {
			minhash = x
		}
		if j == 0 || x > maxhash {
			maxhash = x
		}
	}

	return minhash, maxhash
}

type qrec struct {
	pos  uint32
	hash uint64
	ch   int
}

type prec struct {
	pos  uint32
	hash uint64
}

func genhash(salt [][]byte, infile string, wg *sync.WaitGroup) {

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

	nhash := len(salt)

	// Set up a scanner to read long lines
	scanner := bufio.NewScanner(gzr)
	sbuf := make([]byte, 1024*1024)
	scanner.Buffer(sbuf, 1024*1024)

	// Create files to store the results.
	outmin := make([]*os.File, nhash)
	outmax := make([]*os.File, nhash)
	for j := 0; j < nhash; j++ {
		oname := path.Join(outdirname, fmt.Sprintf("min%04d.bin", j))
		outmin[j], err = os.Create(oname)
		if err != nil {
			panic(err)
		}
		defer outmin[j].Close()
		oname = path.Join(outdirname, fmt.Sprintf("max%04d.bin", j))
		outmax[j], err = os.Create(oname)
		if err != nil {
			panic(err)
		}
		defer outmax[j].Close()
	}

	minchan := make(chan qrec)
	maxchan := make(chan qrec)
	concurrency := 100
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
					p := prec{r.pos, r.hash}
					err = binary.Write(outmin[r.ch], binary.LittleEndian, &p)
					if err != nil {
						panic(err)
					}
				}
			case r, ok := <-maxchan:
				if !ok {
					maxchan = nil
				} else {
					p := prec{r.pos, r.hash}
					err = binary.Write(outmax[r.ch], binary.LittleEndian, &p)
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
		gsq := toks[1]

		limit <- true
		go func(isq []uint8, lnum int) {
			for ci := 0; ci < nhash; ci++ {
				minhash, maxhash := gethash([]byte(gsq), salt[ci])
				if minhash < minthresh {
					minchan <- qrec{uint32(lnum), minhash, ci}
				} else if maxhash > maxthresh {
					maxchan <- qrec{uint32(lnum), maxhash, ci}
				}
			}
			<-limit
		}([]byte(gsq), lnum)
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

	salt := genSalt()

	minthresh = hashbase / 1000000
	maxthresh = hashbase - minthresh

	var wg sync.WaitGroup
	wg.Add(2)
	go genhash(salt, sourcefile, &wg)
	go genhash(salt, targetfile, &wg)
	wg.Wait()
}
