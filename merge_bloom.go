// Merge the sorted match results from the Bloom filter with the
// sorted read sequence file (which also contains counts).  Doing this
// achieves two goals: false positives from the Bloom filter are
// eliminated, and the counts from the sequence file are incorporated
// into the match file.

package main

import (
	"bufio"
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
)

var (
	logger *log.Logger
)

type breader struct {
	scanner *bufio.Scanner
	chunk   [][]string
	stash   []string
	done    bool
	lnum    int
	name    string
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
			return true
		}
		b.chunk = append(b.chunk, fields)
	}
}

func searchpairs(source, match *breader, wtr io.Writer) {

	for _, schunk := range source.chunk {
		for _, mchunk := range match.chunk {

			stag := schunk[0]
			slft := schunk[1]
			srgt := schunk[2]
			scnt := schunk[3]
			//mtag := mchunk[0]
			mlft := mchunk[1]
			mrgt := mchunk[2]
			mgene := mchunk[3]
			mpos := mchunk[4]

			if len(srgt) > len(mrgt) {
				continue
			}
			m := len(srgt)
			if mlft != slft || mrgt[0:m] != srgt[0:m] {
				continue
			}

			mposi, err := strconv.Atoi(mpos)
			if err != nil {
				panic(err)
			}

			wtr.Write([]byte(fmt.Sprintf("%s\t", slft+stag+srgt)))
			wtr.Write([]byte(fmt.Sprintf("%d\t", mposi-len(mlft))))
			wtr.Write([]byte(fmt.Sprintf("%s\t", scnt)))
			wtr.Write([]byte(fmt.Sprintf("%s\n", mgene)))
		}
	}
}

func setupLog() {
	fid, err := os.Create("merge_bloom.log")
	if err != nil {
		panic(err)
	}
	logger = log.New(fid, "", log.Lshortfile)
}

func main() {

	if len(os.Args) != 2 {
		panic("wrong number of arguments")
	}
	setupLog()

	sourcefile := os.Args[1]
	matchfile := strings.Replace(sourcefile, "_sorted.txt.sz", "_smatch.txt.sz", -1)
	outfile := strings.Replace(matchfile, "smatch", "rmatch", -1)
	outfile = strings.Replace(outfile, ".sz", "", -1)

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

lp:
	for {
		s := source.chunk[0][0]
		m := match.chunk[0][0]
		c := strings.Compare(s, m)
		switch {
		case c == 0:
			searchpairs(source, match, out)
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
}
