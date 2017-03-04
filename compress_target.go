// Convert the gene sequence file to a simple compressed format with
// one sequence per row.  The format of each row is:
// sequence<tab>identifier.

package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"log"
	"os"

	"github.com/golang/snappy"
	"github.com/kshedden/seqmatch/utils"
	"github.com/syndtr/goleveldb/leveldb"
)

var (
	logger *log.Logger

	// Database mapping integer gene id (row number in sequence
	// file) to full gene name.
	db *leveldb.DB

	config *utils.Config
)

func targets(sourcefile string) {

	// Setup for reading the input file
	inf, err := os.Open(sourcefile)
	if err != nil {
		panic(err)
	}
	defer inf.Close()

	// Setup for writing the output
	out, err := os.Create(config.GeneFileName)
	if err != nil {
		panic(err)
	}
	defer out.Close()
	outw := snappy.NewBufferedWriter(out)
	defer outw.Close()

	// Setup a scanner to read long lines
	scanner := bufio.NewScanner(inf)
	sbuf := make([]byte, 1024*1024)
	scanner.Buffer(sbuf, 1024*1024)

	ibuf := make([]byte, 4)

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

		binary.LittleEndian.PutUint32(ibuf, uint32(lnum))
		err = db.Put(ibuf, nam, nil)
		if err != nil {
			panic(err)
		}

		_, err = outw.Write(seq)
		if err != nil {
			panic(err)
		}
		_, err = outw.Write([]byte("\n"))
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

	sourcefile := os.Args[2]

	setupLog()

	os.RemoveAll(config.GeneIdDB)
	var err error
	db, err = leveldb.OpenFile(config.GeneIdDB, nil)
	if err != nil {
		logger.Print(err)
		panic(err)
	}
	defer db.Close()

	targets(sourcefile)
	logger.Printf("Done")
}
