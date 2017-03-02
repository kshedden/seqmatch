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
	"path"
	"strconv"
	"strings"

	"github.com/golang/snappy"
)

const (
	// Path to all data files
	dpath string = "/scratch/andjoh_fluxm/tealfurn/CSCAR"

	// Exact line length of input file
	lw int = 150

	concurrency = 100
)

var (
	logger *log.Logger

	// Pool of reusable byte slices
	pool chan []byte

	nmiss int

	rsltChan chan []byte
)

type rec struct {
	buf    []byte
	fields [][]byte
}

func (r *rec) release() {
	pool <- r.buf
	r.buf = nil
}

func (r *rec) init() {
	if r.buf != nil {
		panic("cannot init non-nil rec")
	}
	select {
	case r.buf = <-pool:
	default:
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
		n, err := b.reader.Read(rx.buf)
		if err == io.EOF {
			b.done = true
			logger.Printf("%s done\n", b.name)
			return true
		} else if err != nil {
			panic(err)
		}
		if n != lw {
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
			bbuf := NewBuffer(buf)

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

func setupLog(fname string) {
	toks := strings.Split(fname, "_")
	m := len(toks)
	logname := toks[m-4] + "_" + toks[m-3] + "_" + toks[m-2]
	logname = "merge_bloom_" + logname + ".log"

	fid, err := os.Create(logname)
	if err != nil {
		panic(err)
	}
	logger = log.New(fid, "", log.Lshortfile)
}

func main() {

	var err error
	nmiss, err = strconv.Atoi(os.Args[2])
	if err != nil {
		panic(err)
	}

	sourcefile := os.Args[1]
	matchfile := strings.Replace(sourcefile, "_sorted.txt.sz", "_smatch.txt.sz", -1)

	s := fmt.Sprintf("_%d_rmatch.txt", nmiss)
	outfile := strings.Replace(matchfile, "_smatch.txt.sz", s, -1)
	setupLog(outfile)

	pool = make(chan []byte)

	// Read source sequences
	fn := path.Join(dpath, sourcefile)
	fid, err := os.Open(fn)
	if err != nil {
		panic(err)
	}
	defer fid.Close()
	szr := snappy.NewReader(fid)
	source := &breader{reader: szr, name: "source"}

	// Read candidate match sequences
	gid, err := os.Open(path.Join(dpath, matchfile))
	if err != nil {
		panic(err)
	}
	defer gid.Close()
	szq := snappy.NewReader(gid)
	match := &breader{reader: szq, name: "match"}

	// Place to write results
	out, err := os.Create(path.Join(dpath, outfile))
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
			pool <- r[0:lw]
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
