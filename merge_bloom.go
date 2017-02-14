package main

import (
	"bufio"
	"compress/gzip"
	"fmt"
	"os"
	"path"
	"strconv"
	"strings"
)

const (
	// File containing sequence reads we are matching (source of matching)
	sourcefile string = "PRT_NOV_15_02_sorted.txt.gz"

	// Candidate matching sequences
	matchfile string = "bloom_matches_sorted.txt.gz"

	outfile string = "refined_matches.txt.gz"

	// Path to all data files
	dpath string = "/scratch/andjoh_fluxm/tealfurn/CSCAR"

	// Merge based on this sequence length
	sw = 80
)

func madvance(scanner *bufio.Scanner) (bool, string, string, int) {
	if !scanner.Scan() {
		return true, "", "", -1
	}
	line := scanner.Text()
	fields := strings.Fields(line)
	seq := fields[0]
	target := fields[1]
	pos, err := strconv.Atoi(fields[2])
	if err != nil {
		panic(err)
	}
	return false, seq, target, pos
}

func sadvance(scanner *bufio.Scanner) (bool, string, int) {
	if !scanner.Scan() {
		return true, "", -1
	}
	line := scanner.Text()
	fields := strings.Fields(line)
	seq := fields[1]
	cnt, err := strconv.Atoi(fields[0])
	if err != nil {
		panic(err)
	}
	return false, seq, cnt
}

func main() {

	// Read source sequences
	fid, err := os.Open(path.Join(dpath, sourcefile))
	if err != nil {
		panic(err)
	}
	defer fid.Close()
	gzr, err := gzip.NewReader(fid)
	if err != nil {
		panic(err)
	}
	defer gzr.Close()
	sscan := bufio.NewScanner(gzr)

	// Read candidate match sequences
	gid, err := os.Open(path.Join(dpath, matchfile))
	if err != nil {
		panic(err)
	}
	defer gid.Close()
	szr, err := gzip.NewReader(gid)
	if err != nil {
		panic(err)
	}
	defer szr.Close()
	mscan := bufio.NewScanner(szr)

	// Place to write results
	out, err := os.Create(path.Join(dpath, outfile))
	if err != nil {
		panic(err)
	}
	defer out.Close()
	wtr := gzip.NewWriter(out)
	defer wtr.Close()

	var done bool
	var sseq, mseq, mtarget string
	var mpos, scnt int

	_, sseq, scnt = sadvance(sscan)
	_, mseq, mtarget, mpos = madvance(mscan)

	for !done {

		if len(mseq) < sw {
			_, mseq, mtarget, mpos = madvance(mscan)
			continue
		}

		if len(sseq) < sw {
			_, sseq, scnt = sadvance(sscan)
			continue
		}

		c := strings.Compare(sseq[0:sw], mseq[0:sw])
		switch {
		case c == 0:
			k := 0
			for ; k < 120; k++ {
				if k >= len(sseq) || k >= len(mseq) || sseq[k] != mseq[k] {
					break
				}
			}
			if k >= len(sseq) {
				wtr.Write([]byte(fmt.Sprintf("%d\t", mtarget)))
				wtr.Write([]byte(fmt.Sprintf("%d\t", mpos)))
				wtr.Write([]byte(fmt.Sprintf("%d\t", scnt)))
				wtr.Write([]byte(fmt.Sprintf("%s\n", sseq[0:k])))
			}
			done, mseq, mtarget, mpos = madvance(mscan)
		case c < 0:
			done, sseq, scnt = sadvance(sscan)
		case c > 0:
			done, mseq, mtarget, mpos = madvance(mscan)
		}
	}
}
