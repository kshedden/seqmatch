// Convert the gene sequence file to a simple compressed format with
// one sequence per row.  The format of each row is:
// sequence<tab>identifier.

package main

import (
	"bufio"
	"flag"
	"log"
	"os"
	"path"
	"runtime/pprof"
	"strings"

	"github.com/golang/snappy"
)

const (
	// TODO make this configurable
	dpath string = "/scratch/andjoh_fluxm/tealfurn/CSCAR"
)

var (
	logger *log.Logger
)

func targets(targetfile string) {

	outfile := strings.Replace(targetfile, ".txt", "_tr.txt.sz", -1)

	// Setup for reading the input file
	inf, err := os.Open(path.Join(dpath, targetfile))
	if err != nil {
		panic(err)
	}
	defer inf.Close()

	// Setup for writing the output
	out, err := os.Create(path.Join(dpath, outfile))
	if err != nil {
		panic(err)
	}
	defer out.Close()
	outw := snappy.NewBufferedWriter(out)
	defer outw.Close()

	// Setup a scanner to read long lines
	scanner := bufio.NewScanner(inf)
	sbuf := make([]byte, 1024*1024)
	scanner.Buffer(sbuf, 1024*1024)

	for lnum := 0; scanner.Scan(); lnum++ {

		if lnum%1000000 == 0 {
			logger.Printf("%d\n", lnum)
		}

		line := scanner.Text()
		if err := scanner.Err(); err != nil {
			panic(err)
		}

		toks := strings.Split(line, "\t")
		nam := toks[0]
		seq := []byte(toks[1])

		for i, c := range seq {
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
				seq[i] = 'X'
			}
		}

		_, err = outw.Write(seq)
		if err != nil {
			panic(err)
		}
		_, err = outw.Write([]byte("\t"))
		if err != nil {
			panic(err)
		}
		_, err = outw.Write([]byte(nam))
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

	fid, err := os.Create("compress_target.log")
	if err != nil {
		panic(err)
	}
	logger = log.New(fid, "", log.Lshortfile)
}

func main() {

	cpuprofile := flag.String("cpuprofile", "", "write cpu profile to file")
	targetfile := flag.String("targetfile", "", "gene sequence file")
	flag.Parse()
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	setupLog()
	targets(*targetfile)
	logger.Printf("Done")
}
