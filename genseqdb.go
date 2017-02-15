// Create two leveldb databases containing the sequence data, one for
// the source sequences and one for the target sequences.  This script
// removes any existing databases and starts from an empty database.
//
// For the source sequences, the key is the first `sw` (see below)
// letters of the sequence, and the corresponding value is the
// remaining letters, followed by a tab, followed by the weight.
// Sequences shorter than `sw` letters are not stored.
//
// For the targets, the key is the gene id, and the value is the
// sequence.
//
// Run the script using either "source" or "target" as an argument.

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

	// The sources (~100bp reads)
	sourcefile string = "PRT_NOV_15_02_sorted.txt.gz"

	// The targets (reference gene sequences)
	targetfile string = "ALL_ABFVV_Genes_Derep_derep.txt.gz"

	// Source sequences are stored based on the first sw letters.
	sw int = 80
)

var (
	// The dataset we are working on (source or target)
	dset string

	// A log
	logger *log.Logger
)

func generate(dset string) {

	var infname, dbdir string
	if dset == "source" {
		infname = sourcefile
		dbdir = "source_seqdb"
	} else if dset == "target" {
		infname = targetfile
		dbdir = "target_seqdb"
	}

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

	fid, err := os.Open(path.Join(dpath, infname))
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
			seq := toks[1]
			if len(seq) < sw {
				continue
			}
			wgt := toks[0]
			val := seq[80:len(seq)] + "\t" + wgt
			err := db.Put([]byte(seq[0:80]), []byte(val), nil)
			if err != nil {
				panic(err)
			}
		case "target":
			toks := strings.Split(line, "\t")
			err := db.Put([]byte(toks[0]), []byte(toks[1]), nil)
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

	if len(os.Args) != 2 {
		print("geneseqdb: argument required\n")
	}

	dset = os.Args[1]

	switch dset {
	case "source":
		setupLogger("source")
		generate("source")
	case "target":
		setupLogger("target")
		generate("target")
	default:
		print(fmt.Sprintf("geneseqdb invalid option: %v\n", os.Args[1]))
	}
}
