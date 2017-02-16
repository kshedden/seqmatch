package main

import (
	"compress/gzip"
	"fmt"
	"log"
	"os"
	"path"
	"strconv"
	"strings"

	"github.com/kshedden/seqmatch/utils"
)

const (
	dpath string = "/scratch/andjoh_fluxm/tealfurn/CSCAR"
)

var (
	logger *log.Logger

	// Only the sequence between positions k1 and k2 is retained.
	// Sequence with length less than k2 are skipped.
	k1, k2 int
)

// Compress but restructure to have the same format as the target file
// (one line per sequence).
func source(sourcefile string) {

	outfile := strings.Replace(sourcefile, ".fastq", ".txt.gz", -1)

	s := fmt.Sprintf("_%d_%d", k1, k2)
	outfile = strings.Replace(outfile, ".txt.gz", s+".txt.gz", -1)

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

		if len(ris.Seq) < k2 {
			continue
		}

		x := []byte(ris.Seq[k1:k2])
		for i, c := range x {
			switch c {
			case 'A':
				// pass
			case 'T':
				// pass
			case 'C':
				// pass
			case 'G':
				// pass
			default:
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

	if len(os.Args) != 4 {
		panic("wrong number of arguments\n")
	}
	sourcefile := os.Args[1]

	var err error
	k1, err = strconv.Atoi(os.Args[2])
	if err != nil {
		panic(err)
	}
	k2, err = strconv.Atoi(os.Args[3])
	if err != nil {
		panic(err)
	}

	setupLog()
	source(sourcefile)
	logger.Printf("Done")
}
