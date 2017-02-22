// This script takes raw read file (after sorting and
// removing/counting duplicates) and generates a new file in which
// each row has three fields separated by tab characters.  The first
// field is a subsequence of the original full sequence, beginning and
// ending at positions provided by command-line arguments.  The second
// field is the full original sequence, the third field is the count
// of the full read.  If the full read ends before the end of the
// selected window, it is skipped.

package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"path"
	"strconv"
	"strings"

	"github.com/golang/snappy"
)

const (
	dpath string = "/scratch/andjoh_fluxm/tealfurn/CSCAR"
)

var (
	logger *log.Logger
)

func setupLog() {

	fid, err := os.Create("window_reads.log")
	if err != nil {
		panic(err)
	}
	logger = log.New(fid, "", log.Lshortfile)
}

func main() {

	setupLog()

	if len(os.Args) != 4 {
		panic("wrong number of arguments")
	}

	q1, err := strconv.Atoi(os.Args[2])
	if err != nil {
		panic(err)
	}
	q2, err := strconv.Atoi(os.Args[3])
	if err != nil {
		panic(err)
	}

	// Setup input reader
	infile := os.Args[1]
	fid, err := os.Open(path.Join(dpath, infile))
	if err != nil {
		panic(err)
	}
	defer fid.Close()
	rdr := snappy.NewReader(fid)

	// Setup output writer
	s := fmt.Sprintf("_win_%d_%d.txt.sz", q1, q2)
	outfile := strings.Replace(infile, "_sorted.txt.sz", s, -1)
	gid, err := os.Create(path.Join(dpath, outfile))
	if err != nil {
		panic(err)
	}
	defer gid.Close()
	wtr := snappy.NewBufferedWriter(gid)
	defer wtr.Close()

	// Setup input scanner
	scanner := bufio.NewScanner(rdr)
	buf := make([]byte, 1024*1024)
	scanner.Buffer(buf, 1024*1024)

	for jj := 0; scanner.Scan(); jj++ {

		if jj%1000000 == 0 {
			logger.Printf("%s %d\n", infile, jj)
		}

		line := scanner.Text()
		toks := strings.Fields(line)
		seq := toks[1]
		cnt := toks[0]

		if q2 > len(seq) {
			continue
		}

		key := seq[q1:q2]
		left := seq[0:q1]
		right := seq[q2:len(seq)]
		line = key + "\t" + left + "\t" + right + "\t" + cnt + "\n"
		_, err := wtr.Write([]byte(line))
		if err != nil {
			panic(err)
		}
	}
}
