package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"log"
	"os"
	"path"
	"strings"
)

const (
	dpath string = "/scratch/andjoh_fluxm/tealfurn/CSCAR"
)

var (
	logger *log.Logger
)

func targets(targetfile string) {

	outfile := targetfile + ".gz"

	inf, err := os.Open(path.Join(dpath, targetfile))
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

	// Set up a scanner to read long lines
	scanner := bufio.NewScanner(inf)
	sbuf := make([]byte, 1024*1024)
	scanner.Buffer(sbuf, 1024*1024)

	for lnum := 0; scanner.Scan(); lnum++ {

		if lnum%1000000 == 0 {
			logger.Printf("targets: %d\n", lnum)
		}

		line := scanner.Text()
		if err := scanner.Err(); err != nil {
			panic(err)
		}

		toks := strings.Split(line, "\t")
		nam := toks[0]
		seq := []byte(toks[1])

		for i, c := range seq {
			if !bytes.Contains([]byte("ATGC"), []byte{c}) {
				seq[i] = 'X'
			}
		}

		_, err = outw.Write([]byte(nam + "\t"))
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

	logger.Printf("Done with targets")
}

func setupLog() {

	fid, err := os.Create("compress.log")
	if err != nil {
		panic(err)
	}
	logger = log.New(fid, "", log.Lshortfile)
}

func main() {

	if len(os.Args) != 2 {
		panic("wrong number of arguments\n")
	}
	targetfile := os.Args[1]

	setupLog()
	targets(targetfile)
	logger.Printf("Done")
}
