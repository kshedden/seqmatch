// Create two leveldb databases containing the sequence data, one for
// the source sequences and one for the target sequences.  This script
// removes any existing database and starts from an empty database.
//
// For the source sequences, the key is the first sequence (which is
// usually a subsequence of the full read), and the corresponding
// value is the weight.
//
// For the targets, the key is the gene id, and the value is the
// sequence.
//
// Run the script using either "source" or "target" as an argument
// followed by the raw file name.

package main

import (
	"bufio"
	"compress/gzip"
	"fmt"
	"log"
	"os"
	"path"
	"strings"

	"github.com/syndtr/goleveldb/leveldb"
)

const (
	// Path to all data files
	dpath string = "/scratch/andjoh_fluxm/tealfurn/CSCAR"
)

var (
	// The dataset we are working on (source or target)
	dset string

	// A log
	logger *log.Logger
)

func generate(dset, infile string) {

	var dbdir string
	if dset == "source" {
		dbdir = strings.Replace(infile, "_sorted.txt.gz", "_seqdb", -1)
	} else if dset == "target" {
		dbdir = "target_seqdb"
	}

	// Open an empty database
	dbpath := path.Join(dpath, dbdir)
	err := os.RemoveAll(dbpath)
	if err != nil {
		panic(err)
	}
	db, err := leveldb.OpenFile(dbpath, nil)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	// Open the input sequence file
	fid, err := os.Open(path.Join(dpath, infile))
	if err != nil {
		panic(err)
	}
	defer fid.Close()
	rdr, err := gzip.NewReader(fid)
	if err != nil {
		panic(err)
	}
	defer rdr.Close()

	// Set up a scanner to read long lines
	scanner := bufio.NewScanner(rdr)
	if dset == "target" {
		sbuf := make([]byte, 1024*1024)
		scanner.Buffer(sbuf, 1024*1024)
	}

	for lnum := 0; scanner.Scan(); lnum++ {

		if lnum%1000000 == 0 {
			logger.Printf("%d", lnum)
		}

		line := scanner.Text()

		switch dset {
		case "source":
			toks := strings.Fields(line)
			wgt := toks[0]
			seq := toks[1]
			err := db.Put([]byte(seq), []byte(wgt), nil)
			if err != nil {
				panic(err)
			}
		case "target":
			toks := strings.Split(line, "\t")
			ky := toks[0]
			if len(ky) > 100 {
				ky = ky[0:100] + "..."
			}

			// Get a unique key
			ky0 := ky
			var jj int
			for ; ; jj++ {
				_, err := db.Get([]byte(ky0), nil)
				if err == leveldb.ErrNotFound {
					ky = ky0
					break
				} else if err != nil {
					panic(err)
				}
				// Get a new key
				ky0 = fmt.Sprintf("%s[%d]", ky, jj+1)
			}
			if jj == 1000 {
				panic("Unable to find unique key")
			}

			err := db.Put([]byte(ky), []byte(toks[1]), nil)
			if err != nil {
				panic(err)
			}
		}
	}

	if err := scanner.Err(); err != nil {
		panic(err)
	}

	logger.Printf("%s done", dset)
}

func setupLogger(dset string) {
	fid, err := os.Create(fmt.Sprintf("genseqdb_%s.log", dset))
	if err != nil {
		panic(err)
	}
	logger = log.New(fid, "", log.Lshortfile)
}

func main() {

	if len(os.Args) != 3 {
		print("wrong number of arguments\n")
	}
	dset = os.Args[1]

	switch dset {
	case "source":
		setupLogger("source")
		generate("source", os.Args[2])
	case "target":
		setupLogger("target")
		generate("target", os.Args[2])
	default:
		panic("invalid option")
	}
}
