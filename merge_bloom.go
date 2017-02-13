package main

import (
	"bufio"
	"compress/gzip"
	"os"
	"path"
	"strings"
)

const (
	// File containing sequence reads we are matching (source of matching)
	sourcefile string = "PRT_NOV_15_02_sorted.txt.gz"

	// Candidate matching sequences
	matchfile string = "bloom_matches.txt.gz"

	// Path to all data files
	dpath string = "/scratch/andjoh_fluxm/tealfurn/CSCAR"

	// Merge based on this sequence length
	sw = 80
)

func advance(scanner *bufio.Scanner) (bool, string, []string, string) {
	if !scanner.Scan() {
		return true, "", nil, ""
	}
	line := scanner.Text()
	fields := strings.Fields(line)
	seq := fields[1][0:sw]
	return false, line, fields, seq
}

func main() {

	// Read source sequences
	fid, err := os.Open(path.Join(dpath, sourcefile))
	if err != nil {
		panic(err)
	}
	defer fid.Close()
	gzr := gzip.NewReader(fid)
	defer gzr.Close()
	sscan := bufio.Scanner(gzr)

	// Read candidate match sequences
	gid, err := os.Open(path.Join(dpath, matchfile))
	if err != nil {
		panic(err)
	}
	defer gid.Close()
	szr := gzip.NewReader(gid)
	defer szr.Close()
	mscan := bufio.Scanner(szr)

	// Place to write results
	out, err := os.Create(path.Join(dpath, outfile))
	if err != nil {
		panic(err)
	}
	defer out.Close()
	wtr := gzip.NewWriter(out)
	defer wtr.Close()

	var done bool
	_, sline, sfields, sseq = advance(sscan)
	_, mline, mfields, mseq = advance(mscan)

	for {
		c := strings.Compare(sseq, mseq)

		switch {
		case c == 0:
			out.Write([]byte(mfields[1]))
			out.Write([]byte("\t"))
			out.Write([]byte(sfields[0]))
			out.Write([]byte("\t"))
			out.Write([]byte(sfields[1]))
			out.Write([]byte("\n"))
		case c < 0:
			done, sline, sfields, sseq = advance(sscan)
			if done {
				break
			}
		case c > 0:
			done, mline, mfields, mseq = advance(mscan)
			if done {
				break
			}
		}
	}
}
