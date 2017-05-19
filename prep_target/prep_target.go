// Convert a gene sequence file to a simple format for subsequent
// processing.  The ids and sequences are placed into separate files,
// with one id or sequence per row.
//
// The input can be either a fasta file, or a text format with each
// line containing an id followed by a tab followed by a sequence.

package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/golang/snappy"
)

const (
	// Maximum sequence length.  If there are sequences longer
	// than this, the program will exit with an error.
	maxline int = 1024 * 1024
)

var (
	// If true, data are fasta format, else they are a format with
	// one line per sequence with format id<tab>sequence.
	fasta bool

	logger *log.Logger
)

func processText(scanner *bufio.Scanner, idout, seqout io.Writer) {

	logger.Print("Processing basic text format file...")

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
		_, err := seqout.Write(seq)
		if err != nil {
			panic(err)
		}
		_, err = seqout.Write([]byte("\n"))
		if err != nil {
			panic(err)
		}

		// Write the gene id
		_, err = idout.Write([]byte(fmt.Sprintf("%011d\t", lnum)))
		if err != nil {
			panic(err)
		}
		_, err = idout.Write(nam)
		if err != nil {
			panic(err)
		}
		_, err = idout.Write([]byte(fmt.Sprintf("\t%d\n", len(seq))))
		if err != nil {
			panic(err)
		}
	}

	if err := scanner.Err(); err != nil {
		panic(err)
	}
}

func processFasta(scanner *bufio.Scanner, idout, seqout io.Writer) {

	logger.Print("Processing basic text format file...")

	var seqname string
	var seq []string
	var lnum int

	flush := func() {

		// Write the sequence
		for _, s := range seq {
			_, err := seqout.Write([]byte(s))
			if err != nil {
				panic(err)
			}
		}
		_, err := seqout.Write([]byte("\n"))
		if err != nil {
			panic(err)
		}

		// Write the gene id
		_, err = idout.Write([]byte(fmt.Sprintf("%011d\t", lnum)))
		if err != nil {
			panic(err)
		}
		_, err = idout.Write([]byte(seqname))
		if err != nil {
			panic(err)
		}
		_, err = idout.Write([]byte(fmt.Sprintf("\t%d\n", len(seq))))
		if err != nil {
			panic(err)
		}
	}

	for lnum = 0; scanner.Scan(); lnum++ {

		if lnum%1000000 == 0 {
			logger.Printf("%d\n", lnum)
		}

		line := scanner.Text()

		if line[0] == '>' {
			if len(seq) > 0 {
				flush()
			}
			seqname = line
			seq = seq[0:0]
			continue
		}

		// Replace non A/T/G/C with X
		bline := []byte(line)
		for i, c := range bline {
			switch c {
			case 'A':
			case 'T':
			case 'C':
			case 'G':
			default:
				bline[i] = 'X'
			}
		}
		seq = append(seq, string(bline))
	}

	if err := scanner.Err(); err != nil {
		panic(err)
	}

	if len(seq) > 0 {
		flush()
	}
}

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
	idwtr, err := os.Create(geneidfile)
	if err != nil {
		panic(err)
	}
	defer idwtr.Close()
	idout := snappy.NewBufferedWriter(idwtr)
	defer idout.Close()

	// Setup a scanner to read long lines
	scanner := bufio.NewScanner(inf)
	sbuf := make([]byte, maxline)
	scanner.Buffer(sbuf, maxline)

	if fasta {
		processFasta(scanner, idout, seqout)
	} else {
		processText(scanner, idout, seqout)
	}

	logger.Printf("Done processing targets")
}

func setupLog() {
	fid, err := os.Create("compress_target.log")
	if err != nil {
		panic(err)
	}
	logger = log.New(fid, "", log.Ltime)
}

func main() {

	if len(os.Args) != 2 {
		os.Stderr.WriteString("prep_target: wrong number of arguments\n")
		os.Stderr.WriteString("Usage:\n")
		os.Stderr.WriteString("  prep_target genefile\n\n")
		os.Exit(1)
	}

	genefile := os.Args[1]

	gl := strings.ToLower(genefile)
	if strings.HasSuffix(gl, "fasta") {
		fasta = true
	}

	setupLog()

	targets(genefile)
	logger.Printf("Done")
}
