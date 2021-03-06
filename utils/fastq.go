package utils

import (
	"bufio"
	"os"
	"path"
)

// ReadInSeq reads the sequencing reads, returns names and sequences
type ReadInSeq struct {
	file    *os.File
	scanner *bufio.Scanner
	Name    string
	Seq     string
}

func NewReadInSeq(seqfile, dpath string) *ReadInSeq {
	inf, err := os.Open(path.Join(dpath, seqfile))
	if err != nil {
		panic(err)
	}

	scanner := bufio.NewScanner(inf)

	return &ReadInSeq{
		file:    inf,
		scanner: scanner,
	}
}

func (ris *ReadInSeq) Next() bool {

	for j := 0; j < 4; j++ {

		if !ris.scanner.Scan() {

			if err := ris.scanner.Err(); err != nil {
				panic(err)
			}

			return false
		}

		switch j % 4 {
		case 0:
			ris.Name = ris.scanner.Text()
		case 1:
			ris.Seq = ris.scanner.Text()
		}

		if err := ris.scanner.Err(); err != nil {
			panic(err)
		}
	}

	return true
}
