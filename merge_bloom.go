// Merge the sorted match results from the Bloom filter with the
// sorted read sequence file (which also contains counts).  Doing this
// achieves two goals: false positives from the Bloom filter are
// eliminated, and the counts from the sequence file are incorporated
// into the match file.

package main

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/golang/snappy"
	"github.com/kshedden/seqmatch/utils"
)

const (
	// Exact line length of input file
	lw int = 150

	concurrency = 100
)

var (
	logger *log.Logger

	config *utils.Config

	pmiss float64

	// Pool of reusable byte slices
	pool chan []byte

	// The window to process and its start/end position
	win int
	q1  int
	q2  int

	rsltChan chan []byte
)

type rec struct {
	buf    []byte
	fields [][]byte
}

func (r *rec) release() {
	if r.buf == nil {
		panic("nothing to release")
	}
	select {
	case pool <- r.buf:
	default:
		// pool is full, dump the buffer to the garbage
	}
	r.buf = nil
}

func (r *rec) init() {
	if r.buf != nil {
		panic("cannot init non-nil rec")
	}
	select {
	case r.buf = <-pool:
	default:
		// Pool is empty, make a new buffer
		r.buf = make([]byte, lw)
	}
}

func (r *rec) setfields() {
	r.fields = bytes.Fields(r.buf)
}

type breader struct {
	reader io.Reader
	recs   []*rec
	stash  *rec
	done   bool
	lnum   int
	name   string

	// Used to confirm that file is sorted
	last *rec
}

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

	for ii := 0; ; ii++ {

		// Read a line
		rx := new(rec)
		rx.init()
		n, err := io.ReadFull(b.reader, rx.buf)
		if err == io.EOF {
			b.done = true
			logger.Printf("%s done\n", b.name)
			return true
		} else if err != nil {
			panic(err)
		}
		if n != lw {
			fmt.Printf("%v\n", rx.buf[135])
			fmt.Printf("%d\n", len(rx.buf))
			fmt.Printf("n=%d\n", n)
			panic("short line")
		}
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
					panic("file is not sorted")
				}
			}
			b.last = rx
			b.recs = append(b.recs, rx)
		}
	}
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

func searchpairs(source, match []*rec, sem chan bool) {

	for _, mrec := range match {

		//mtag := mrec.fields[0]
		mlft := mrec.fields[1]
		mrgt := mrec.fields[2]
		mgene := mrec.fields[3]
		mpos := mrec.fields[4]

		for _, srec := range source {

			stag := srec.fields[0] // must equal mtag
			slft := srec.fields[1]
			srgt := srec.fields[2]
			scnt := srec.fields[3]

			nmiss := int(pmiss * float64(len(stag)+len(slft)+len(srgt)))

			// Gene ends before read would end, can't match.
			if len(srgt) > len(mrgt) {
				continue
			}

			// Count differences
			m := len(srgt)
			nx := cdiff(mlft, slft)
			nx += cdiff(mrgt[0:m], srgt[0:m])
			if nx > nmiss {
				continue
			}

			// unavoidable []byte to string copy
			mposi, err := strconv.Atoi(string(mpos))
			if err != nil {
				panic(err)
			}

			var buf []byte
			select {
			case buf = <-pool:
			default:
				buf = make([]byte, lw)
			}
			bbuf := bytes.NewBuffer(buf)

			bbuf.Write(slft)
			bbuf.Write(stag)
			bbuf.Write(srgt)
			x := fmt.Sprintf("\t%d\t%s\t%s\n", mposi-len(mlft), scnt, mgene)
			bbuf.Write([]byte(x))
			rsltChan <- bbuf.Bytes()
		}
	}
	if len(match)*len(source) > 1000 {
		logger.Printf("searched %d %d", len(match), len(source))
	}
	for _, x := range source {
		x.release()
	}
	for _, x := range match {
		x.release()
	}
	<-sem
}

func setupLog(win int) {
	s := fmt.Sprintf("_mergebloom_%d.log", win)
	logname := strings.Replace(config.ReadFileName, ".fastq", s, 1)

	fid, err := os.Create(logname)
	if err != nil {
		panic(err)
	}
	logger = log.New(fid, "", log.Lshortfile)
}

func main() {

	if len(os.Args) != 3 {
		panic("wrong number of arguments")
	}

	config = utils.ReadConfig(os.Args[1])

	var err error
	win, err = strconv.Atoi(os.Args[2])
	if err != nil {
		panic(err)
	}

	pmiss, err = strconv.ParseFloat(os.Args[2], 64)
	if err != nil {
		panic(err)
	}

	q1 = config.Windows[win]
	q2 = q1 + config.WindowWidth
	s := fmt.Sprintf("_win_%d_%d.txt.sz", q1, q2)
	sourcefile := strings.Replace(config.ReadFileName, ".fastq", s, 1)
	matchfile := strings.Replace(sourcefile, "_sorted.txt.sz", "_smatch.txt.sz", 1)

	s = fmt.Sprintf("_%.0f_rmatch.txt", 100*config.PMiss)
	outfile := strings.Replace(config.ReadFileName, ".fastq", s, 1)
	setupLog(win)

	pool = make(chan []byte, 10000)

	// Read source sequences
	fid, err := os.Open(sourcefile)
	if err != nil {
		panic(err)
	}
	defer fid.Close()
	szr := snappy.NewReader(fid)
	source := &breader{reader: szr, name: "source"}

	// Read candidate match sequences
	gid, err := os.Open(matchfile)
	if err != nil {
		panic(err)
	}
	defer gid.Close()
	szq := snappy.NewReader(gid)
	match := &breader{reader: szq, name: "match"}

	// Place to write results
	out, err := os.Create(outfile)
	if err != nil {
		panic(err)
	}
	defer out.Close()

	source.Next()
	match.Next()

	// Harvest the results
	go func() {
		for r := range rsltChan {
			out.Write(r)
			select {
			case pool <- r[0:cap(r)]:
			default:
				// pool is full, buffer goes to garbage
			}
		}
	}()

	rsltChan = make(chan []byte)
	sem := make(chan bool, concurrency)

lp:
	for ii := 0; ; ii++ {

		s := source.recs[0].fields[0]
		m := match.recs[0].fields[0]
		c := bytes.Compare(s, m)

		switch {
		case c == 0:
			sem <- true
			go searchpairs(source.recs, match.recs, sem)
			source.recs = source.recs[0:0] // don't release memory, searchpairs will do it
			match.recs = match.recs[0:0]
			ms := source.Next()
			mb := match.Next()
			if !(ms && mb) {
				break lp
			}
		case c < 0:
			ms := source.Next()
			if !ms {
				break lp
			}
		case c > 0:
			mb := match.Next()
			if !mb {
				break lp
			}
		}
	}

	logger.Print("clearing channel")
	for k := 0; k < concurrency; k++ {
		sem <- true
	}

	close(rsltChan)

	logger.Print("done")
}
