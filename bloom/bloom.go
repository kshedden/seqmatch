// Identify possible matches of a set of source sequences into a set
// of target sequences.
//
// The result may contain false positives, but will not have any false
// negatives.

package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"path"
	"strings"

	"github.com/chmduquesne/rollinghash"
	"github.com/chmduquesne/rollinghash/buzhash"
	"github.com/golang-collections/go-datastructures/bitarray"
	"github.com/golang/snappy"
	"github.com/kshedden/seqmatch/utils"
)

const (
	// Number of goroutines
	concurrency int = 100

	// Line length for output
	lw int = 150
)

var (
	// A log
	logger *log.Logger

	config *utils.Config

	// Bitarrays that back the Bloom filter
	smp []bitarray.BitArray

	// Tables to produce independent running hashes
	tables [][256]uint32

	// Communicate results back to driver
	hitchan chan rec

	// Semaphore for limiting goroutines
	limit chan bool
)

func genTables() {
	tables = make([][256]uint32, config.NumHash)
	for j := 0; j < config.NumHash; j++ {
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

	hashes := make([]rollinghash.Hash32, config.NumHash)
	for j, _ := range hashes {
		hashes[j] = buzhash.NewFromByteArray(tables[j])
	}

	d, f := path.Split(config.ReadFileName)
	f = strings.Replace(f, ".fastq", "_sorted.txt.sz", 1)
	fname := path.Join(d, "tmp", f)
	fid, err := os.Open(fname)
	if err != nil {
		logger.Print(err)
		panic(err)
	}
	defer fid.Close()
	snr := snappy.NewReader(fid)
	scanner := bufio.NewScanner(snr)

	wk := make([]int, 25)

	for j := 0; scanner.Scan(); j++ {

		if j%1000000 == 0 {
			logger.Printf("%d\n", j)
		}

		line := scanner.Bytes()
		toks := bytes.Fields(line)
		seq := toks[1]

		for k := 0; k < len(config.Windows); k++ {
			q1 := config.Windows[k]
			q2 := q1 + config.WindowWidth
			if q2 > len(seq) {
				continue
			}
			seqw := seq[q1:q2]
			if utils.CountDinuc(seqw, wk) < config.MinDinuc {
				continue
			}

			for _, ha := range hashes {
				ha.Reset()
				ha.Write(seqw)
				x := uint64(ha.Sum32()) % config.BloomSize
				err := smp[k].SetBit(x)
				if err != nil {
					logger.Print(err)
					panic(err)
				}
			}
		}
	}

	if err := scanner.Err(); err != nil {
		logger.Print(err)
		panic(err)
	}

	logger.Printf("Done constructing filters")
}

type rec struct {
	mseq  string
	left  string
	right string
	win   int
	tnum  int
	pos   uint32
}

// checkwin returns the indices of the Bloom filters that match the
// current state of the hashes.
func checkwin(ix []int, iw []uint64, hashes []rollinghash.Hash32) []int {

	ix = ix[0:0]
	for j, ha := range hashes {
		iw[j] = uint64(ha.Sum32()) % config.BloomSize
	}

	for k, ba := range smp {
		g := true
		for j, _ := range hashes {
			f, err := ba.GetBit(iw[j])
			if err != nil {
				logger.Print(err)
				panic(err)
			}
			if !f {
				g = false
				break
			}
		}
		if g {
			ix = append(ix, k)
		}
	}

	return ix
}

func processseq(seq []byte, genenum int) {

	hashes := make([]rollinghash.Hash32, config.NumHash)
	for j, _ := range hashes {
		hashes[j] = buzhash.NewFromByteArray(tables[j])
	}

	hlen := config.WindowWidth
	for j, _ := range hashes {
		hashes[j].Write(seq[0:hlen])
	}
	ix := make([]int, len(smp))
	iw := make([]uint64, config.NumHash)

	// Check if the initial window is a match
	ix = checkwin(ix, iw, hashes)
	for _, i := range ix {

		q1 := config.Windows[i]
		if q1 != 0 {
			continue
		}
		q2 := q1 + config.WindowWidth

		jz := 100 - q2
		if jz > len(seq) {
			jz = len(seq)
		}
		hitchan <- rec{
			mseq:  string(seq[0:hlen]),
			left:  "",
			right: string(seq[hlen:jz]),
			tnum:  genenum,
			win:   i,
			pos:   0,
		}
	}

	// Check the rest of the windows
	for j := hlen; j < len(seq); j++ {

		for _, ha := range hashes {
			ha.Roll(seq[j])
		}
		ix = checkwin(ix, iw, hashes)

		// Process a match
		for _, i := range ix {

			q1 := config.Windows[i]
			q2 := q1 + config.WindowWidth
			if j < q2-1 {
				continue
			}

			// Matching sequence is jx:jy
			jx := j - hlen + 1
			jy := j + 1

			// Left tail is jw:jx
			jw := jx - q1

			// Right tail is jy:jz
			jz := jy + 100 - q2
			if jz > len(seq) {
				// May not be long enough, but we don't know until we merge.
				jz = len(seq)
			}

			if jw >= 0 {
				hitchan <- rec{
					mseq:  string(seq[jx:jy]),
					left:  string(seq[jw:jx]),
					right: string(seq[jy:jz]),
					tnum:  genenum,
					win:   i,
					pos:   uint32(j - hlen + 1),
				}
			}
		}
	}
	<-limit
}

// Retrieve the results and write to disk
func harvest(wtrs []io.Writer) {

	bb := bytes.Repeat([]byte(" "), lw)
	bb[lw-1] = byte('\n')

	for r := range hitchan {

		wtr := wtrs[r.win]

		n1, err1 := wtr.Write([]byte(fmt.Sprintf("%s\t", r.mseq)))
		n2, err2 := wtr.Write([]byte(fmt.Sprintf("%s\t", r.left)))
		n3, err3 := wtr.Write([]byte(fmt.Sprintf("%s\t", r.right)))
		n4, err4 := wtr.Write([]byte(fmt.Sprintf("%011d\t", r.tnum)))
		n5, err5 := wtr.Write([]byte(fmt.Sprintf("%d", r.pos)))

		for _, err := range []error{err1, err2, err3, err4, err5} {
			if err != nil {
				logger.Print(err)
				panic("writing error")
			}
		}

		n := n1 + n2 + n3 + n4 + n5
		if n > lw {
			panic("output line is too long")
		}

		_, err := wtr.Write(bb[n:lw])
		if err != nil {
			logger.Print(err)
			panic(err)
		}
	}
}

func search() {

	fid, err := os.Open(config.GeneFileName)
	if err != nil {
		logger.Print(err)
		panic(err)
	}
	defer fid.Close()
	snr := snappy.NewReader(fid)

	// Target file contains some very long lines
	scanner := bufio.NewScanner(snr)
	sbuf := make([]byte, 1024*1024)
	scanner.Buffer(sbuf, 1024*1024)

	hitchan = make(chan rec)
	limit = make(chan bool, concurrency)

	var wtrs []io.Writer
	for k := 0; k < len(config.Windows); k++ {
		q1 := config.Windows[k]
		d, f := path.Split(config.ReadFileName)
		s := fmt.Sprintf("_%d_%d_bmatch.txt.sz", q1, q1+config.WindowWidth)
		f = strings.Replace(f, ".fastq", s, 1)
		outname := path.Join(d, "tmp", f)
		out, err := os.Create(outname)
		if err != nil {
			logger.Print(err)
			panic(err)
		}
		defer out.Close()
		wtr := snappy.NewBufferedWriter(out)
		defer wtr.Close()
		wtrs = append(wtrs, wtr)
	}
	go harvest(wtrs)

	for i := 0; scanner.Scan(); i++ {

		if i%1000000 == 0 {
			logger.Printf("%d\n", i)
		}

		line := scanner.Text() // need a copy here
		if err := scanner.Err(); err != nil {
			logger.Print(err)
			panic(err)
		}

		toks := strings.Split(line, "\t")
		seq := toks[0]

		limit <- true
		go processseq([]byte(seq), i)
	}

	for k := 0; k < concurrency; k += 1 {
		limit <- true
	}

	close(hitchan)
	logger.Printf("done with search")
}

func setupLogger() {
	d, f := path.Split(config.ReadFileName)
	f = strings.Replace(f, ".fastq", "_bloom.log", 1)
	logname := path.Join(d, "tmp", f)
	logfid, err := os.Create(logname)
	if err != nil {
		logger.Print(err)
		panic(err)
	}
	logger = log.New(logfid, "", log.Lshortfile)
}

func estimateFullness() {

	n := 1000
	logger.Printf("Bloom filter fill rates:\n")

	for j, ba := range smp {
		c := 0
		for k := 0; k < n; k++ {
			i := uint64(rand.Int63()) % config.BloomSize
			f, err := ba.GetBit(i)
			if err != nil {
				panic(err)
			}
			if f {
				c++
			}
		}
		logger.Printf("%3d %.3f\n", j, float64(c)/float64(n))
	}
}

func main() {

	if len(os.Args) != 2 {
		panic("wrong number of arguments")
	}

	config = utils.ReadConfig(os.Args[1])

	setupLogger()
	genTables()

	smp = make([]bitarray.BitArray, len(config.Windows))
	for k, _ := range smp {
		smp[k] = bitarray.NewBitArray(config.BloomSize)
	}

	buildBloom()
	estimateFullness()
	search()
}
