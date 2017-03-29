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
	"bytes"
	"fmt"
	"io"
	"log"
	"os"
	"path"

	"github.com/golang/snappy"
	"github.com/kshedden/seqmatch/utils"
)

var (
	logger *log.Logger

	tmpdir string

	config *utils.Config
)

func setupLog() {
	logname := path.Join(tmpdir, "window_reads.log")
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

	if config.TempDir == "" {
		tmpdir = os.Args[2]
	} else {
		tmpdir = config.TempDir
	}

	setupLog()

	// Setup input reader
	fname := path.Join(tmpdir, "reads_sorted.txt.sz")
	fid, err := os.Open(fname)
	if err != nil {
		panic(err)
	}
	defer fid.Close()
	rdr := snappy.NewReader(fid)

	// Setup input scanner
	scanner := bufio.NewScanner(rdr)
	buf := make([]byte, 1024*1024)
	scanner.Buffer(buf, 1024*1024)

	// Setup output writers
	var wtrs []io.Writer
	for k := 0; k < len(config.Windows); k++ {
		f := fmt.Sprintf("win_%d.txt.sz", k)
		outfile := path.Join(tmpdir, f)
		gid, err := os.Create(outfile)
		if err != nil {
			panic(err)
		}
		defer gid.Close()
		wtr := snappy.NewBufferedWriter(gid)
		defer wtr.Close()
		wtrs = append(wtrs, wtr)
	}

	wk := make([]int, 25)

	for jj := 0; scanner.Scan(); jj++ {

		if jj%1000000 == 0 {
			logger.Printf("%d\n", jj)
		}

		line := scanner.Bytes() // don't need copy
		toks := bytes.Fields(line)
		seq := toks[1]
		cnt := toks[0]

		var bbuf bytes.Buffer
		for k := 0; k < len(config.Windows); k++ {

			q1 := config.Windows[k]
			q2 := q1 + config.WindowWidth

			// Sequence is too short
			if len(seq) < q2 {
				continue
			}

			key := seq[q1:q2]
			if utils.CountDinuc(key, wk) < config.MinDinuc {
				continue
			}

			bbuf.Reset()
			_, err1 := bbuf.Write(key)
			_, err2 := bbuf.Write([]byte("\t"))
			_, err3 := bbuf.Write(seq[0:q1])
			_, err4 := bbuf.Write([]byte("\t"))
			_, err5 := bbuf.Write(seq[q2:len(seq)])
			_, err6 := bbuf.Write([]byte("\t"))
			_, err7 := bbuf.Write(cnt)
			_, err8 := bbuf.Write([]byte("\n"))

			for _, e := range []error{err1, err2, err3, err4, err5, err6, err7, err8} {
				if e != nil {
					logger.Print(e)
					panic(e)
				}
			}

			_, err := wtrs[k].Write(bbuf.Bytes())
			if err != nil {
				logger.Print(err)
				panic(err)
			}
		}
	}
}
