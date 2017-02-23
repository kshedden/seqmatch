// Merge the sorted match results from the Bloom filter with the
// sorted read sequence file (which also contains counts).  Doing this
// achieves two goals: false positives from the Bloom filter are
// eliminated, and the counts from the sequence file are incorporated
// into the match file.

package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"path"
	"runtime"
	"strconv"
	"strings"

	"github.com/golang/snappy"
)

const (
	// Path to all data files
	dpath string = "/scratch/andjoh_fluxm/tealfurn/CSCAR"

	concurrency = 100
)

var (
	logger *log.Logger

	nmiss int

	rsltChan chan string
)

type breader struct {
	scanner *bufio.Scanner
	chunk   [][]string
	stash   []string
	done    bool
	lnum    int
	name    string

	// Used to confirm that file is sorted
	last    string
	haslast bool
}

func (b *breader) Next() bool {

	if b.done {
		return false
	}

	b.chunk = b.chunk[0:0]
	if b.stash != nil {
		b.chunk = append(b.chunk, b.stash)
		b.stash = nil
	}

	for {
		f := b.scanner.Scan()
		if err := b.scanner.Err(); err != nil {
			panic(err)
		}
		b.lnum++
		if b.lnum%100000 == 0 {
			logger.Printf("%s: %d\n", b.name, b.lnum)
		}

		if !f {
			b.done = true
			logger.Printf("%s done\n", b.name)
			return true
		}

		line := b.scanner.Text()
		fields := strings.Split(line, "\t")

		if (b.chunk != nil) && (fields[0] != b.chunk[0][0]) {
			b.stash = fields
			if b.haslast {
				if b.last > b.chunk[0][0] {
					panic("file is not sorted")
				}
			}
			b.last = b.chunk[0][0]
			b.haslast = true
			return true
		}
		b.chunk = append(b.chunk, fields)
	}
}

func cdiff(x, y []byte) int {
	var c int
	for i, v := range x {
		if v != y[i] {
			c++
		}
	}
	return c
}

func searchpairs(source, match [][]string, sem chan bool) {

	for _, mrec := range match {

		//mtag := mchunk[0]
		mlft := mrec[1]
		mrgt := mrec[2]
		mgene := mrec[3]
		mpos := mrec[4]

		for _, srec := range source {

			stag := srec[0] // must equal mtag
			slft := srec[1]
			srgt := srec[2]
			scnt := srec[3]

			// Gene ends before read would end, can't match.
			if len(srgt) > len(mrgt) {
				continue
			}

			m := len(srgt)
			nx := cdiff([]byte(mlft), []byte(slft))
			nx += cdiff([]byte(mrgt[0:m]), []byte(srgt[0:m]))
			if nx > nmiss {
				continue
			}

			mposi, err := strconv.Atoi(mpos)
			if err != nil {
				panic(err)
			}

			rslt := fmt.Sprintf("%s\t", slft+stag+srgt)
			rslt += fmt.Sprintf("%d\t", mposi-len(mlft))
			rslt += fmt.Sprintf("%s\t", scnt)
			rslt += fmt.Sprintf("%s\n", mgene)
			rsltChan <- rslt
		}
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

func cpy(x [][]string) [][]string {
	y := make([][]string, len(x))
	for i, v := range x {
		y[i] = make([]string, len(v))
		copy(y[i], v)
	}
	return y
}

func main() {

	if len(os.Args) != 3 {
		panic("wrong number of arguments")
	}

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

	// Read source sequences
	fn := path.Join(dpath, sourcefile)
	fid, err := os.Open(fn)
	if err != nil {
		panic(err)
	}
	defer fid.Close()
	szr := snappy.NewReader(fid)
	sscan := bufio.NewScanner(szr)
	sbuf := make([]byte, 1024*1024)
	sscan.Buffer(sbuf, 1024*1024)
	source := &breader{scanner: sscan, name: "source"}

	// Read candidate match sequences
	gid, err := os.Open(path.Join(dpath, matchfile))
	if err != nil {
		panic(err)
	}
	defer gid.Close()
	szq := snappy.NewReader(gid)
	mscan := bufio.NewScanner(szq)
	sbuf = make([]byte, 1024*1024)
	mscan.Buffer(sbuf, 1024*1024)
	match := &breader{scanner: mscan, name: "match"}

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
			out.Write([]byte(r))
		}
	}()

	var ms runtime.MemStats
	rsltChan = make(chan string)
	sem := make(chan bool, concurrency)

lp:
	for ii := 0; ; ii++ {

		if ii%100000 == 0 {
			runtime.ReadMemStats(&ms)
			logger.Printf("memory: %+v\n", ms.Alloc)
		}

		s := source.chunk[0][0]
		m := match.chunk[0][0]

		c := strings.Compare(s, m)
		switch {
		case c == 0:
			sem <- true
			go searchpairs(cpy(source.chunk), cpy(match.chunk), sem)
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

	for k := 0; k < concurrency; k++ {
		sem <- true
	}

	close(rsltChan)

	logger.Print("done")
}
