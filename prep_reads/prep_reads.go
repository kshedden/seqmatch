// Convert a source file of sequencing reads from fastq format to a
// simple format with one sequence per row.

package main

import (
	"bytes"
	"os"

	"github.com/kshedden/seqmatch/utils"
)

var (
	config *utils.Config

	tmpdir string
)

func source() {

	ris := utils.NewReadInSeq(config.ReadFileName, "")

	var bbuf bytes.Buffer

	var lnum int
	for lnum = 0; ris.Next(); lnum++ {

		bbuf.Reset()

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

		if len(xseq) > config.MaxReadLength {
			xseq = xseq[0:config.MaxReadLength]
		}

		bbuf.Write(xseq)
		bbuf.Write([]byte("\t"))

		rn := ris.Name
		if len(rn) > 1000 {
			rn = rn[0:995] + "..."
		}
		bbuf.Write([]byte(rn))

		bbuf.Write([]byte("\n"))

		_, err := os.Stdout.Write(bbuf.Bytes())
		if err != nil {
			panic(err)
		}
	}
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

	source()
}
