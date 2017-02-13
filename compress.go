package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"log"
	"os"
	"path"
	"strings"
	"sync"

	"github.com/kshedden/seqmatch/utils"
)

const (
	targetfile string = "ALL_ABFVV_Genes_Derep.txt"

	sourcefile string = "PRT_NOV_15_02.fastq"

	dpath string = "/scratch/andjoh_fluxm/tealfurn/CSCAR"

	gzlevel int = 1
)

var (
	logger *log.Logger
)

// Compress but restructure to have the same format as the target file
// (one line per sequence).
func source(wg *sync.WaitGroup) {

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
	outw, err := gzip.NewWriterLevel(out, gzlevel)
	if err != nil {
		panic(err)
	}
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

	wg.Done()
	logger.Printf("Done with sources")
}

func targets(wg *sync.WaitGroup) {

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
	outw, err := gzip.NewWriterLevel(out, gzlevel)
	if err != nil {
		panic(err)
	}
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

	wg.Done()
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

	var wg sync.WaitGroup

	setupLog()

	wg.Add(2)
	go source(&wg)
	go targets(&wg)
	wg.Wait()
	logger.Printf("Done")
}
