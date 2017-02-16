// Merge the sorted match results from the Bloom filter with the
// sorted read sequence file (which also contains counts).  Doing this
// achieves two goals: false positives from the Bloom filter are
// eliminated, and the counts from the sequence file are incorporated
// into the match file.

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
	// Path to all data files
	dpath string = "/scratch/andjoh_fluxm/tealfurn/CSCAR"
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

	if len(os.Args) != 2 {
		panic("wrong number of arguments")
	}

	sourcefile := os.Args[1]
	if !strings.HasSuffix(sourcefile, "_sorted.txt.gz") {
		panic("wrong input file")
	}
	matchfile := strings.Replace(sourcefile, "_sorted.txt.gz", "_smatch.txt.gz", -1)
	outfile := strings.Replace(matchfile, "smatch", "rmatch", -1)

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
	sbuf := make([]byte, 1024*1024)
	sscan.Buffer(sbuf, 1024*1024)

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
	sbuf = make([]byte, 1024*1024)
	mscan.Buffer(sbuf, 1024*1024)

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
		c := strings.Compare(sseq, mseq)
		switch {
		case c == 0:
			if len(mtarget) > 100 {
				mtarget = mtarget[0:100] + "..."
			}
			wtr.Write([]byte(fmt.Sprintf("%s\t", mtarget)))
			wtr.Write([]byte(fmt.Sprintf("%d\t", mpos)))
			wtr.Write([]byte(fmt.Sprintf("%d\t", scnt)))
			wtr.Write([]byte(fmt.Sprintf("%s\n", sseq)))
			done, mseq, mtarget, mpos = madvance(mscan)
		case c < 0:
			done, sseq, scnt = sadvance(sscan)
		case c > 0:
			done, mseq, mtarget, mpos = madvance(mscan)
		}
	}

	// Check for scanning errors
	err = mscan.Err()
	if err != nil {
		panic(err)
	}
	err = sscan.Err()
	if err != nil {
		panic(err)
	}
}
