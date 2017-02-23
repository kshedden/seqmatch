// Convert a source file of sequencing reads from fastq format to a
// simple compressed format with one sequence per row.

package main

import (
	"log"
	"os"
	"path"
	"strings"

	"github.com/golang/snappy"
	"github.com/kshedden/seqmatch/utils"
)

const (
	// TODO: make this configurable
	dpath string = "/scratch/andjoh_fluxm/tealfurn/CSCAR"
)

var (
	logger *log.Logger
)

func source(sourcefile string) {

	outfile := strings.Replace(sourcefile, ".fastq", ".txt.sz", -1)
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
	outw := snappy.NewBufferedWriter(out)
	defer outw.Close()

	ris := utils.NewReadInSeq(sourcefile, dpath)

	for lnum := 0; ris.Next(); lnum++ {

		if lnum%1000000 == 0 {
			logger.Printf("sources: %d\n", lnum)
		}

		var na, nt int
		xseq := []byte(ris.Seq)
		for i, c := range xseq {
			switch c {
			case 'A':
				na++
			case 'T':
				nt++
			case 'C':
				// pass
			case 'G':
				// pass
			default:
				xseq[i] = 'X'
			}
		}

		// Drop if poly-A or poly-T TODO: the constant should
		// be configurable
		if na > len(xseq)-5 || nt > len(xseq)-5 {
			continue
		}

		_, err := outw.Write(xseq)
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
