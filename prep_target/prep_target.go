// Convert the gene sequence file to a simple compressed format with
// one sequence per row.  The identifiers are stored in a separate
// file.

package main

import (
	"bufio"
	"bytes"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/golang/snappy"
)

const (
	maxline int = 1024 * 1024
)

var (
	logger *log.Logger
)

func targets(genefile string) {

	// Setup for reading the input file
	inf, err := os.Open(genefile)
	if err != nil {
		panic(err)
	}
	defer inf.Close()

	// Setup for writing the sequence output
	ext := filepath.Ext(genefile)
	geneoutfile := strings.Replace(genefile, ext, ".txt.sz", 1)
	gid1, err := os.Create(geneoutfile)
	if err != nil {
		panic(err)
	}
	defer gid1.Close()
	seqout := snappy.NewBufferedWriter(gid1)
	defer seqout.Close()

	// Setup for writing the identifier output
	geneidfile := strings.Replace(genefile, ext, "_ids.txt.sz", 1)
	gid2, err := os.Create(geneidfile)
	if err != nil {
		panic(err)
	}
	defer gid2.Close()
	idout := snappy.NewBufferedWriter(gid2)
	defer idout.Close()

	// Setup a scanner to read long lines
	scanner := bufio.NewScanner(inf)
	sbuf := make([]byte, maxline)
	scanner.Buffer(sbuf, maxline)

	for lnum := 0; scanner.Scan(); lnum++ {

		if lnum%1000000 == 0 {
			logger.Printf("%d\n", lnum)
		}

		line := scanner.Bytes()
		toks := bytes.Split(line, []byte("\t"))
		nam := toks[0]
		seq := toks[1]

		// Replace non A/T/G/C with X
		for i, c := range seq {
			switch c {
			case 'A':
			case 'T':
			case 'C':
			case 'G':
			default:
				seq[i] = 'X'
			}
		}

		// Write the sequence
		_, err = seqout.Write(seq)
		if err != nil {
			panic(err)
		}
		_, err = seqout.Write([]byte("\n"))
		if err != nil {
			panic(err)
		}

		// Write the gene id
		_, err = idout.Write([]byte(fmt.Sprintf("%011d ", lnum)))
		if err != nil {
			panic(err)
		}
		_, err = idout.Write(nam)
		if err != nil {
			panic(err)
		}
		_, err = idout.Write([]byte("\n"))
		if err != nil {
			panic(err)
		}
	}

	if err := scanner.Err(); err != nil {
		panic(err)
	}

	logger.Printf("Done with targets")
}

func setupLog() {
	fid, err := os.Create("compress_target.log")
	if err != nil {
		panic(err)
	}
	logger = log.New(fid, "", log.Lshortfile)
}

func main() {

	if len(os.Args) != 2 {
		panic("wrong number of arguments")
	}

	genefile := os.Args[1]

	setupLog()

	targets(genefile)
	logger.Printf("Done")
}
