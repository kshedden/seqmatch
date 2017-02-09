// Generate a collection of minhash/maxhash values for all source and
// target sequences.  Only the tails of the distribution are saved.

package main

import (
	"bufio"
	"compress/gzip"
	"encoding/binary"
	"log"
	"math"
	"math/rand"
	"os"
	"path"
	"strings"
	"sync"

	"github.com/ktye/fft"
)

const (
	// Number of hashes
	nhash int = 100

	// The kmer length of each hash
	hlen int = 20

	concurrency int = 10

	// Only keep if the minhash/maxhash is beyond this threshold in magnitude
	minthresh float64 = -2
	maxthresh float64 = 2
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

	// The hash footprints
	hashfp [][]float64

	// A resource pool
	pool       sync.Mutex
	workspaces map[int][]*workspace
)

// copy a float slice into the real components of a complex slice
func floatcomplex(x []float64, z []complex128) {
	for i, v := range x {
		z[i] = complex(v, 0)
	}
}

type workspace struct {
	fft   *fft.FFT
	z     []complex128
	s     []complex128
	f     [][]complex128
	inuse bool
}

func newWorkspace(n int) *workspace {

	fft, err := fft.New(n)
	if err != nil {
		panic(err)
	}

	w := workspace{
		fft: &fft,
		z:   make([]complex128, n),
		s:   make([]complex128, n),
		f:   make([][]complex128, nhash),
	}
	for j, f := range hashfp {
		w.f[j] = make([]complex128, n)
		floatcomplex(f, w.f[j])
		w.fft.Transform(w.f[j])
	}

	return &w
}

func codeSeq(seq string, z []complex128) {
	for i, c := range seq {
		switch c {
		case 'A':
			z[i] = complex(-2/math.Sqrt(9), 0)
		case 'T':
			z[i] = complex(-1/math.Sqrt(9), 0)
		case 'G':
			z[i] = complex(1/math.Sqrt(9), 0)
		case 'C':
			z[i] = complex(2/math.Sqrt(9), 0)
		}
	}
	for j := len(seq); j < len(z); j++ {
		z[j] = complex(0, 0)
	}
}

// z <- x*y
func mulTo(z, x, y []complex128) {
	for i, v := range x {
		z[i] = v * y[i]
	}
}

// Returns the minimum and maximum of the hash applied to the given sequence
func gethashes(seqid int, seq string, minchan, maxchan chan qrec) {

	n := len(seq)

	// Round up to the nearest power of 2.
	n2 := int(math.Pow(2, math.Ceil(math.Log(float64(n))/math.Log(2))))

	// Get a workspace from the resource pool, or create one if needed
	pool.Lock()
	var w *workspace
	for _, wx := range workspaces[n2] {
		if !wx.inuse {
			wx.inuse = true
			w = wx
			break
		}
	}
	if w == nil {
		w = newWorkspace(n2)
		w.inuse = true
		workspaces[n2] = append(workspaces[n2], w)
	}
	pool.Unlock()

	// FT of the data sequence
	codeSeq(seq, w.s)
	w.fft.Transform(w.s)

	for j := 0; j < nhash; j++ {

		// Convolution via FFT
		mulTo(w.z, w.f[j], w.s)
		w.fft.Inverse(w.z)

		min := real(w.z[hlen-1])
		max := real(w.z[hlen-1])
		for i := hlen; i < n; i++ {
			if real(w.z[i]) < min {
				min = real(w.z[i])
			}
			if real(w.z[i]) > max {
				max = real(w.z[i])
			}
		}
		if min < minthresh {
			minchan <- qrec{uint32(seqid), min, uint16(j)}
		}
		if max > maxthresh {
			maxchan <- qrec{uint32(seqid), max, uint16(j)}
		}
	}

	pool.Lock()
	w.inuse = false
	pool.Unlock()
}

type qrec struct {
	seqid   uint32
	hashval float64
	hashid  uint16
}

func genhash(infile string, wg *sync.WaitGroup) {

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

		if lnum%10000 == 0 {
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
		go func(seq string, lnum int) {
			gethashes(lnum, seq, minchan, maxchan)
			<-limit
		}(seq, lnum)
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

func genhashfp() {
	hashfp = make([][]float64, nhash)
	for j := 0; j < nhash; j++ {
		hashfp[j] = make([]float64, hlen)
		for i := 0; i < hlen; i++ {
			hashfp[j][i] = rand.NormFloat64() / math.Sqrt(float64(hlen))
		}
	}
}

func main() {

	setupLogger()
	genhashfp()
	workspaces = make(map[int][]*workspace)

	var wg sync.WaitGroup
	wg.Add(2)
	//genhash(sourcefile, &wg)
	genhash(targetfile, &wg)
	//wg.Wait()
}
