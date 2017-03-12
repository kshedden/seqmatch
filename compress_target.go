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

	"github.com/golang/snappy"
	"github.com/kshedden/seqmatch/utils"
)

var (
	logger *log.Logger

	config *utils.Config
)

func targets(sourcefile string) {

	// Setup for reading the input file
	inf, err := os.Open(sourcefile)
	if err != nil {
		panic(err)
	}
	defer inf.Close()

	// Setup for writing the sequence output
	gid1, err := os.Create(config.GeneFileName)
	if err != nil {
		panic(err)
	}
	defer gid1.Close()
	seqout := snappy.NewBufferedWriter(gid1)
	defer seqout.Close()

	// Setup for writing the identifier output
	gid2, err := os.Create(config.GeneIdFileName)
	if err != nil {
		panic(err)
	}
	defer gid2.Close()
	idout := snappy.NewBufferedWriter(gid2)
	defer idout.Close()

	// Setup a scanner to read long lines
	scanner := bufio.NewScanner(inf)
	sbuf := make([]byte, 1024*1024)
	scanner.Buffer(sbuf, 1024*1024)

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

	if len(os.Args) != 3 {
		panic("wrong number of arguments")
	}

	jsonfile := os.Args[1]
	config = utils.ReadConfig(jsonfile)

	genefile := os.Args[2]

	setupLog()

	targets(genefile)
	logger.Printf("Done")
}
