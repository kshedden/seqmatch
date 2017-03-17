// Merge the sorted match results from the Bloom filter with the
// sorted read sequences.  Doing this achieves two goals: false
// positives from the Bloom filter are eliminated, and the count
// information from the sequence file is incorporated into the match
// file.

package main

import (
	"bufio"
	"bytes"
	"fmt"
	"log"
	"os"
	"path"
	"runtime/pprof"
	"strconv"
	"strings"

	"github.com/golang/snappy"
	"github.com/kshedden/seqmatch/utils"
)

const (
	// Needs to hold two reads
	lw int = 300

	concurrency = 100

	profile = false

	// Maintain a pool of byte arrays of length lw
	poolsize = 10000
)

var (
	logger *log.Logger

	config *utils.Config

	// Pool of reusable byte slices
	pool chan []byte

	win int // The window to process, win=0,1,...
	q1  int // The start position of the window
	q2  int // The end position of the window

	// Pass results to driver then write to disk
	rsltChan chan []byte

	alldone chan bool
)

type rec struct {
	buf    []byte
	fields [][]byte
}

func (r *rec) Print() {
	fmt.Printf("len(buf)=%d\n", len(r.buf))
	for k, f := range r.fields {
		fmt.Printf("%d %s\n", k, string(f))
	}
}

func (r *rec) release() {
	if r.buf == nil {
		logger.Print("nothing to release")
		panic("nothing to release")
	}
	putbuf(r.buf)
	r.buf = nil
	r.fields = nil
}

func (r *rec) init() {
	if r.buf != nil {
		logger.Print("cannot init non-nil rec")
		panic("cannot init non-nil rec")
	}
	r.buf = getbuf()
	r.buf = r.buf[0:0]
}

func (r *rec) setfields() {
	r.fields = bytes.Split(r.buf, []byte("\t"))
}

// breader iterates through a set of sequences, combining blocks of
// contiguous records with the same window sequence.  A breader can be
// used to iterate through either the match or the raw read data.  The
// input sequence windows must be sorted.
type breader struct {

	// The input sequences
	scanner *bufio.Scanner

	// The caller can access the block data through this field
	recs []*rec

	// If we read past the end of a block, put it here so it can
	// be included in the next iteration.
	stash *rec

	// True if all sequences have been read.  At this point, the
	// recs field will continue to hold the final block of
	// sequences.
	done bool

	// The current line number in the input file
	lnum int

	// The ame of the source of sequences (either "match" or
	// "source").
	name string

	// Used to confirm that file is sorted
	last *rec
}

// Next advances a breader to the next block.
func (b *breader) Next() bool {

	if b.done {
		return false
	}

	for _, b := range b.recs {
		b.release()
	}
	b.recs = b.recs[0:0]

	if b.stash != nil {
		b.recs = append(b.recs, b.stash)
		b.stash = nil
	}

	for ii := 0; b.scanner.Scan(); ii++ {

		// Process a line
		bb := b.scanner.Bytes()
		if len(bb) > lw {
			logger.Print("line too long")
			panic("line too long")
		}
		rx := new(rec)
		rx.init()
		rx.buf = rx.buf[0:len(bb)]
		copy(rx.buf, bb)
		rx.setfields()

		b.lnum++
		if b.lnum%100000 == 0 {
			logger.Printf("%s: %d\n", b.name, b.lnum)
		}

		if (len(b.recs) > 0) && !bytes.Equal(b.recs[0].fields[0], rx.fields[0]) {
			b.stash = rx
			return true
		} else {
			// Check sorting (harder to check in other branch of the if).
			if ii > 0 {
				if bytes.Compare(b.last.fields[0], rx.fields[0]) > 0 {
					logger.Print("file is not sorted")
					panic("file is not sorted")
				}
			}
			b.last = rx
			b.recs = append(b.recs, rx)
		}
	}

	if err := b.scanner.Err(); err != nil {
		logger.Print(err)
		panic(err)
	}

	b.done = true
	logger.Printf("%s done", b.name)
	return true
}

// cdiff returns the number of unequal values in two byte sequences
func cdiff(x, y []byte) int {
	var c int
	for i, v := range x {
		if v != y[i] {
			c++
		}
	}
	return c
}

func putbuf(buf []byte) {
	select {
	case pool <- buf[0:0]:
	default:
		// pool is full, buffer goes to garbage
	}
}

func getbuf() []byte {
	var buf []byte
	select {
	case buf = <-pool:
		buf = buf[0:0]
	default:
		buf = make([]byte, 0, lw)
	}
	return buf
}

func searchpairs(source, match []*rec, limit chan bool) {

	defer func() { <-limit }()
	if len(match)*len(source) > 10000 {
		logger.Printf("searching %d %d ...", len(match), len(source))
	}

	var stag []byte
	for _, mrec := range match {

		mtag := mrec.fields[0]
		mlft := mrec.fields[1]
		mrgt := mrec.fields[2]
		mgene := mrec.fields[3]
		mpos := mrec.fields[4]

		for _, srec := range source {

			stag = srec.fields[0] // must equal mtag
			slft := srec.fields[1]
			srgt := srec.fields[2]
			scnt := srec.fields[3]

			nmiss := int((1 - config.PMatch) * float64(len(stag)+len(slft)+len(srgt)))

			// Gene ends before read would end, can't match.
			if len(srgt) > len(mrgt) {
				continue
			}

			// Count differences
			mk := len(srgt)
			nx := cdiff(mlft, slft)
			nx += cdiff(mrgt[0:mk], srgt)
			if nx > nmiss {
				continue
			}

			// unavoidable []byte to string copy
			mposi, err := strconv.Atoi(strings.TrimRight(string(mpos), " "))
			if err != nil {
				logger.Print(err)
				panic(err)
			}

			// Found a match, pass to output
			buf := getbuf()
			bbuf := bytes.NewBuffer(buf)
			bbuf.Write(slft)
			bbuf.Write(stag)
			bbuf.Write(srgt)
			bbuf.Write([]byte("\t"))
			bbuf.Write(mlft)
			bbuf.Write(mtag)
			bbuf.Write(mrgt[0:mk])
			x := fmt.Sprintf("\t%d\t%s\t%s\n", mposi-len(mlft), scnt, mgene)
			bbuf.Write([]byte(x))
			rsltChan <- bbuf.Bytes()

			// one match is enough for now (may need to
			// make this configurable)
			break
		}
	}

	if len(match)*len(source) > 10000 {
		logger.Printf("done with search")
	}

	for _, x := range source {
		x.release()
	}
	for _, x := range match {
		x.release()
	}
}

func setupLog(win int) {
	d, f := path.Split(config.ReadFileName)
	q1 := config.Windows[win]
	q2 := q1 + config.WindowWidth
	s := fmt.Sprintf("_mergebloom_%d_%d_%.0f.log", q1, q2, 100*config.PMatch)
	f = strings.Replace(f, ".fastq", s, 1)
	logname := path.Join(d, "tmp", f)

	fid, err := os.Create(logname)
	if err != nil {
		logger.Print(err)
		panic(err)
	}
	logger = log.New(fid, "", log.Lshortfile)
}

// rcpy deeply copies its argument.
func rcpy(r []*rec) []*rec {
	x := make([]*rec, len(r))
	for j, _ := range x {
		x[j] = new(rec)
		x[j].init()
		x[j].buf = x[j].buf[0:len(r[j].buf)]
		copy(x[j].buf, r[j].buf)
		x[j].setfields()
	}
	return x
}

func main() {

	if len(os.Args) != 3 {
		panic("wrong number of arguments")
	}

	if profile {
		f, err := os.Create("merge_bloom_cpuprof")
		if err != nil {
			panic(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	config = utils.ReadConfig(os.Args[1])

	var err error
	win, err = strconv.Atoi(os.Args[2])
	if err != nil {
		panic(err)
	}
	setupLog(win)

	q1 = config.Windows[win]
	q2 = q1 + config.WindowWidth
	s := fmt.Sprintf("_win_%d_%d_sorted.txt.sz", q1, q2)
	d, f := path.Split(config.ReadFileName)
	f = strings.Replace(f, ".fastq", s, 1)
	sourcefile := path.Join(d, "tmp", f)
	logger.Printf("sourcefile: %s", sourcefile)

	s = fmt.Sprintf("_%d_%d_smatch.txt.sz", q1, q2)
	d, f = path.Split(config.ReadFileName)
	f = strings.Replace(f, ".fastq", s, 1)
	matchfile := path.Join(d, "tmp", f)
	logger.Printf("matchfile: %s", matchfile)

	s = fmt.Sprintf("_%d_%d_%.0f_rmatch.txt.sz", q1, q2, 100*config.PMatch)
	d, f = path.Split(config.ReadFileName)
	f = strings.Replace(f, ".fastq", s, 1)
	outfile := path.Join(d, "tmp", f)
	logger.Printf("outfile: %s", outfile)

	pool = make(chan []byte, poolsize)

	// Read source sequences
	fid, err := os.Open(sourcefile)
	if err != nil {
		logger.Print(err)
		panic(err)
	}
	defer fid.Close()
	szr := snappy.NewReader(fid)
	scanner := bufio.NewScanner(szr)
	source := &breader{scanner: scanner, name: "source"}

	// Read candidate match sequences
	gid, err := os.Open(matchfile)
	if err != nil {
		logger.Print(err)
		panic(err)
	}
	defer gid.Close()
	szq := snappy.NewReader(gid)
	scanner = bufio.NewScanner(szq)
	match := &breader{scanner: scanner, name: "match"}

	// Place to write results
	fi, err := os.Create(outfile)
	if err != nil {
		logger.Print(err)
		panic(err)
	}
	defer fi.Close()
	out := snappy.NewBufferedWriter(fi)
	defer out.Flush()

	source.Next()
	match.Next()

	rsltChan = make(chan []byte, 5*concurrency)
	limit := make(chan bool, concurrency)
	alldone = make(chan bool)

	// Harvest the results
	go func() {
		for r := range rsltChan {
			_, err := out.Write(r)
			if err != nil {
				panic(err)
			}
			putbuf(r)
		}
		alldone <- true
	}()

lp:
	for ii := 0; ; ii++ {

		if profile && ii > 100000 {
			logger.Printf("Breaking early for profile run")
			break
		}

		if ii%100000 == 0 {
			logger.Printf("%d", ii)
		}

		s := source.recs[0].fields[0]
		m := match.recs[0].fields[0]
		c := bytes.Compare(s, m)

		ms := true
		mb := true

		switch {
		case c == 0:
			// Window sequences match, check if it is a real match.
			limit <- true
			go searchpairs(rcpy(source.recs), rcpy(match.recs), limit)
			ms = source.Next()
			mb = match.Next()
			if !(ms || mb) {
				break lp
			}
		case c < 0:
			// The source sequence is behind, move it up.
			ms = source.Next()
			if !ms {
				break lp
			}
		case c > 0:
			// The match sequence is behind, move it up.
			mb = match.Next()
			if !mb {
				break lp
			}
		}
		if !(ms && mb) {
			// One of the files is done
			logger.Printf("ms=%v, mb=%v\n", ms, mb)
		}
	}

	logger.Print("clearing channel")
	for k := 0; k < cap(limit); k++ {
		limit <- true
	}

	close(rsltChan)
	<-alldone

	logger.Print("done")
}
