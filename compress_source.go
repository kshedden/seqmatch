package main

import (
	"bytes"
	"compress/gzip"
	"log"
	"os"
	"path"
	"strings"

	"github.com/kshedden/seqmatch/utils"
)

const (
	dpath string = "/scratch/andjoh_fluxm/tealfurn/CSCAR"
)

var (
	logger *log.Logger
)

// Compress but restructure to have the same format as the target file
// (one line per sequence).
func source(sourcefile string) {

	outfile := strings.Replace(sourcefile, ".fastq", ".txt.gz", -1)

	inf, err := os.Open(path.Join(dpath, sourcefile))
	if err != nil {
		panic(err)
	}
	defer inf.Close()

	out, err := os.Create(path.Join(dpath, outfile))
	if err != nil {
		panic(err)
	}
	defer out.Close()
	outw := gzip.NewWriter(out)
	defer outw.Close()

	ris := utils.NewReadInSeq(sourcefile, dpath)

	for lnum := 0; ris.Next(); lnum++ {

		if lnum%1000000 == 0 {
			logger.Printf("sources: %d\n", lnum)
		}

		x := []byte(ris.Seq)
		for i, c := range x {
			if !bytes.Contains([]byte("ATGC"), []byte{c}) {
				x[i] = 'X'
			}
		}
		_, err := outw.Write(x)
		if err != nil {
			panic(err)
		}

		_, err = outw.Write([]byte("\n"))
		if err != nil {
			panic(err)
		}
	}

	logger.Printf("Done with sources")
}

func setupLog() {
	fid, err := os.Create("compress_source.log")
	if err != nil {
		panic(err)
	}
	logger = log.New(fid, "", log.Lshortfile)
}

func main() {

	if len(os.Args) != 2 {
		panic("wrong number of arguments\n")
	}
	sourcefile := os.Args[1]

	setupLog()
	source(sourcefile)
	logger.Printf("Done")
}
