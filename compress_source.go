// Convert a source file of sequencing reads from fastq format to a
// simple compressed format with one sequence per row.

package main

import (
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/golang/snappy"
	"github.com/kshedden/seqmatch/utils"
)

var (
	config *utils.Config

	logger *log.Logger
)

func source(sourcefile string) {

	outfile := strings.Replace(sourcefile, ".fastq", ".txt.sz", -1)
	inf, err := os.Open(sourcefile)
	if err != nil {
		panic(err)
	}
	defer inf.Close()

	out, err := os.Create(outfile)
	if err != nil {
		panic(err)
	}
	defer out.Close()
	outw := snappy.NewBufferedWriter(out)
	defer outw.Close()

	ris := utils.NewReadInSeq(sourcefile, "")

	for lnum := 0; ris.Next(); lnum++ {

		if lnum%1000000 == 0 {
			logger.Printf("sources: %d\n", lnum)
		}

		if len(ris.Seq) < config.MinReadLength {
			continue
		}

		xseq := []byte(ris.Seq)
		for i, c := range xseq {
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
				xseq[i] = 'X'
			}
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
	v := strings.Split(config.ReadFileName, "/")
	b := v[len(v)-1]
	v = strings.Split(b, ".")
	b = v[0]
	fid, err := os.Create(fmt.Sprintf("%s_compress_source.log", b))
	if err != nil {
		panic(err)
	}
	logger = log.New(fid, "", log.Lshortfile)
}

func main() {
	if len(os.Args) != 2 {
		panic("wrong number of arguments\n")
	}

	config = utils.ReadConfig(os.Args[1])

	setupLog()
	source(config.ReadFileName)
	logger.Printf("Done")
}
