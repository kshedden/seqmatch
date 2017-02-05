package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"sort"
	"strings"

	"github.com/syndtr/goleveldb/leveldb"
)

var (
	// Path to all data files
	dpath string = "/scratch/andjoh_fluxm/tealfurn/CSCAR"

	// The number of hashes
	nhash int

	// A database to store the match candidates
	db *leveldb.DB

	// Path to the match candidate database
	dbdir string = "matchdb"
)

// Determine the number of hashes based on the files in the hash directory.
func setNhash() {
	pt := path.Join(dpath, "source_hashes")
	fi, err := ioutil.ReadDir(pt)
	if err != nil {
		panic(err)
	}
	nhash = 0
	for _, f := range fi {
		fn := f.Name()
		if strings.HasPrefix(fn, "min") && strings.HasSuffix(fn, ".bin") {
			nhash++
		}
	}
}

type prec struct {
	Pos  uint32
	Hash uint64
}

type byhash []prec

func (a byhash) Len() int           { return len(a) }
func (a byhash) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a byhash) Less(i, j int) bool { return a[i].Hash < a[j].Hash }

type vu32 []uint32

func (a vu32) Len() int           { return len(a) }
func (a vu32) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a vu32) Less(i, j int) bool { return a[i] < a[j] }

func uint32ToBytes(dat []uint32, byt []byte) []byte {
	if cap(byt) < 4*len(dat) {
		byt = make([]byte, 4*len(dat))
	}
	byt = byt[0 : 4*len(dat)]

	for i, x := range dat {
		binary.LittleEndian.PutUint32(byt[4*i:4*(i+1)], x)
	}

	return byt
}

// Intersect returns the intersection of sorted arrays a and b
// containing distinct elements.  The array c is a workspace backing
// the returned slice.
func intersect(a, b, c []uint32) []uint32 {

	c = c[0:0]

	var i, j int

	for i < len(a) && j < len(b) {
		switch {
		case a[i] < b[j]:
			i++
		case a[i] > b[j]:
			j++
			if j >= len(b) {
				break
			}
		default:
			c = append(c, a[i])
			i++
			j++
		}
	}

	return c
}

func bytesToUint32(byt []byte, dat []uint32) []uint32 {

	m := len(byt) / 4
	if cap(dat) < m {
		dat = make([]uint32, m)
	}
	dat = dat[0:m]

	buf := bytes.NewReader(byt)
	for k := 0; ; k++ {
		err := binary.Read(buf, binary.LittleEndian, &dat[k])
		if err == io.EOF {
			break
		} else if err != nil {
			panic(err)
		}
	}

	return dat
}

func proc(hx int) {

	fn := fmt.Sprintf("min%04d.bin", hx)

	var source []prec
	var target []prec
	var dir string
	for j := 0; j < 2; j++ {

		var ar []prec
		if j == 0 {
			dir = path.Join(dpath, "source_hashes")
		} else {
			dir = path.Join(dpath, "target_hashes")
		}

		fid, err := os.Open(path.Join(dir, fn))
		if err != nil {
			panic(err)
		}

		for {
			var x prec
			err := binary.Read(fid, binary.LittleEndian, &x)
			if err == io.EOF {
				break
			} else if err != nil {
				panic(err)
			}
			ar = append(ar, x)
		}

		if j == 0 {
			source = ar
		} else {
			target = ar
		}
	}

	sort.Sort(byhash(source))
	sort.Sort(byhash(target))

	fmt.Printf("%v %v\n", len(source), len(target))

	kb := make([]byte, 4)
	var byt []byte
	var u32 []uint32
	var u32a []uint32
	var u32b []uint32

	for _, v := range source {

		j := sort.Search(len(target), func(i int) bool { return target[i].Hash > v.Hash })

		// All the possible matching targets based on this hash
		u32 = u32[0:0]
		for k := 0; k < j; k++ {
			u32 = append(u32, target[k].Pos)
		}
		sort.Sort(vu32(u32))

		// Database key for this source entry
		binary.LittleEndian.PutUint32(kb, v.Pos)

		// Check if there is already a record for this source sequence
		data, err := db.Get(kb, nil)
		if err == leveldb.ErrNotFound {
			byt = uint32ToBytes(u32, byt)
			e := db.Put(kb, byt, nil)
			if e != nil {
				panic(e)
			}
			continue
		} else if err != nil {
			panic(err)
		}

		u32a = bytesToUint32(data, u32a)
		u32b = intersect(u32, u32a, u32b)
		byt = uint32ToBytes(u32b, byt)
		e := db.Put(kb, byt, nil)
		if e != nil {
			panic(e)
		}
	}
}

func main() {

	dbfp := path.Join(dpath, dbdir)
	var err error
	db, err = leveldb.OpenFile(dbfp, nil)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	setNhash()

	for k := 0; k < nhash; k++ {
		proc(k)
	}
}
