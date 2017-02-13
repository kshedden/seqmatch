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

	sw = 80
)

func main() {

	fid, err := os.Open(path.Join(dpath, sourcefile))
	if err != nil {
		panic(err)
	}
	defer fid.Close()
	gzr := gzip.NewReader(fid)
	defer gzr.Close()
	sscan := bufio.Scanner(gzr)

	gid, err := os.Open(path.Join(dpath, matchfile))
	if err != nil {
		panic(err)
	}
	defer gid.Close()
	szr := gzip.NewReader(gid)
	defer szr.Close()
	mscan := bufio.Scanner(szr)

	out, err := os.Create(path.Join(dpath, outfile))
	if err != nil {
		panic(err)
	}
	defer out.Close()
	wtr := gzip.NewWriter(out)
	defer wtr.Close()

	sscan.Scan()
	sline := sscan.Text()
	sfields := strings.Fields(sline)
	sseq := sfields[1][0:sw]

	mscan.Scan()
	mline := mscan.Text()
	mfields := strings.Fields(mline)
	mseq := mfields[0][0:sw]

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
			if !sscan.Scan() {
				break
			}
			sline = sscan.Text()
			sfields = strings.Fields(sline)
			sseq = sfields[1][0:sw]
		case c > 0:
			if !mscan.Scan() {
				break
			}
			mline = mscan.Text()
			mfields = strings.Fields(mline)
			mseq = mfields[0][0:sw]
		}
	}
}
