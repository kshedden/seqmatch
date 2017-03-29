// Convert a source file of sequencing reads from fastq format to a
// simple compressed format with one sequence per row.

package main

import (
	"log"
	"os"
	"path"

	"github.com/golang/snappy"
	"github.com/kshedden/seqmatch/utils"
)

var (
	config *utils.Config

	tmpdir string

	logger *log.Logger
)

func source() {

	outfile := path.Join(tmpdir, "reads.txt.sz")

	out, err := os.Create(outfile)
	if err != nil {
		panic(err)
	}
	defer out.Close()
	outw := snappy.NewBufferedWriter(out)
	defer outw.Close()

	ris := utils.NewReadInSeq(config.ReadFileName, "")

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
	logname := path.Join(tmpdir, "compress_source.log")
	fid, err := os.Create(logname)
	if err != nil {
		panic(err)
	}
	logger = log.New(fid, "", log.Lshortfile)
}

func main() {
	if len(os.Args) != 3 {
		panic("wrong number of arguments\n")
	}

	config = utils.ReadConfig(os.Args[1])

	if config.TempDir == "" {
		tmpdir = os.Args[2]
	} else {
		tmpdir = config.TempDir
	}

	setupLog()
	source()
	logger.Printf("Done")
}
