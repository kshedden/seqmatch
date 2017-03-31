package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"os/exec"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/golang/snappy"
	"github.com/kshedden/seqmatch/utils"
	"golang.org/x/sys/unix"
)

const ()

var (
	jsonfile    string
	tmpjsonfile string
	config      *utils.Config
	basename    string
	tmpdir      string
	pipedir     string
	logger      *log.Logger
)

func compresssource() {
	logger.Printf("starting compresssource")
	cmd := exec.Command("prep_reads", tmpjsonfile, tmpdir)
	cmd.Env = os.Environ()
	cmd.Stderr = os.Stderr
	err := cmd.Run()
	if err != nil {
		panic(err)
	}
	logger.Printf("compresssource done")
}

func pipename() string {
	f := fmt.Sprintf("%09d", rand.Int63()%1e9)
	return path.Join(pipedir, f)
}

func pipefromsz(fname string) string {

	rand.Seed(int64(time.Now().UnixNano() + int64(os.Getpid())))

	for k := 0; k < 10; k++ {
		name := pipename()
		err := unix.Mkfifo(name, 0755)
		if err == nil {
			go func() {
				cmd := exec.Command("sztool", "-d", fname, name)
				cmd.Env = os.Environ()
				cmd.Stderr = os.Stderr
				err := cmd.Run()
				if err != nil {
					panic(err)
				}
			}()
			return name
		}
		print(fmt.Sprintf("%v\n", err))
	}

	panic("unable to create pipe")
}

func sortsource() {

	logger.Printf("starting sortsource")

	fname := path.Join(tmpdir, "reads.txt.sz")
	pname1 := pipefromsz(fname)
	logger.Printf("Reading from %s", fname)

	cmd1 := exec.Command("sort", "-S", "2G", "--parallel=8", pname1)
	cmd1.Env = os.Environ()
	cmd1.Stderr = os.Stderr
	pip, err := cmd1.StdoutPipe()
	if err != nil {
		panic(err)
	}

	err = cmd1.Start()
	if err != nil {
		panic(err)
	}

	scanner := bufio.NewScanner(pip)
	buf := make([]byte, 1024*1024)
	scanner.Buffer(buf, len(buf))

	// File for sequences
	outname := path.Join(tmpdir, "reads_sorted.txt.sz")
	logger.Printf("Writing sequences to %s", outname)
	fid, err := os.Create(outname)
	if err != nil {
		panic(err)
	}
	defer fid.Close()
	wtr := snappy.NewBufferedWriter(fid)
	defer wtr.Close()

	// File for counts/names
	outname = path.Join(tmpdir, "reads_sorted_ids.txt.sz")
	logger.Printf("Writing names to %s", outname)
	fid, err = os.Create(outname)
	if err != nil {
		panic(err)
	}
	defer fid.Close()
	nameswtr := snappy.NewBufferedWriter(fid)
	defer nameswtr.Close()

	// Get the first line
	if !scanner.Scan() {
		logger.Printf("no input")
		panic("no input")
	}
	if err := scanner.Err(); err != nil {
		panic(err)
	}
	fields := strings.Fields(scanner.Text())
	seq := fields[0]
	name := fields[1]
	n := 1

	dowrite := func(seq, name string, n int) {
		_, err = wtr.Write([]byte(seq))
		if err != nil {
			panic(err)
		}
		s := fmt.Sprintf(" %d %s\n", n, name)
		_, err = nameswtr.Write([]byte(s))
		if err != nil {
			panic(err)
		}
	}

	for scanner.Scan() {

		line := scanner.Text()
		fields1 := strings.Fields(line)
		seq1 := fields1[0]
		name1 := fields1[1]

		if strings.Compare(seq, seq1) == 0 {
			n++
			name = fmt.Sprintf("%s;%s", name, name1)
			continue
		}

		dowrite(seq, name, n)
		seq = seq1
		name = name1
		n = 1
	}

	dowrite(seq, name, n)

	if err := scanner.Err(); err != nil {
		panic(err)
	}

	if err := cmd1.Wait(); err != nil {
		log.Fatal(err)
	}

	logger.Printf("sortsource done")
}

func windowreads() {
	logger.Printf("starting windowreads")

	cmd := exec.Command("window_reads", tmpjsonfile, tmpdir)
	cmd.Env = os.Environ()
	cmd.Stderr = os.Stderr
	err := cmd.Run()
	if err != nil {
		panic(err)
	}

	logger.Printf("windowreads done")
}

func sortwindows() {

	logger.Printf("starting sortwindows")
	var cmds []*exec.Cmd

	for k := 0; k < len(config.Windows); k++ {
		f := fmt.Sprintf("win_%d.txt.sz", k)
		fname := path.Join(tmpdir, f)
		pname1 := pipefromsz(fname)

		cmd2 := exec.Command("sort", "-S", "2G", "--parallel=8", "-k1", pname1)
		cmd2.Env = os.Environ()
		cmd2.Stderr = os.Stderr

		fname = strings.Replace(fname, ".txt.sz", "_sorted.txt.sz", 1)
		cmd3 := exec.Command("sztool", "-c", "-", fname)
		cmd3.Env = os.Environ()
		cmd3.Stderr = os.Stderr
		var err error
		cmd3.Stdin, err = cmd2.StdoutPipe()
		if err != nil {
			panic(err)
		}

		cmds = append(cmds, cmd2, cmd3)
	}

	for _, cmd := range cmds {
		err := cmd.Start()
		if err != nil {
			panic(err)
		}
	}
	for _, cmd := range cmds {
		err := cmd.Wait()
		if err != nil {
			panic(err)
		}
	}

	logger.Printf("sortwindows done")
}

func bloom() {
	logger.Printf("starting bloom")
	cmd := exec.Command("bloom", tmpjsonfile, tmpdir)
	cmd.Env = os.Environ()
	cmd.Stderr = os.Stderr
	err := cmd.Run()
	if err != nil {
		panic(err)
	}
	logger.Printf("bloom done")
}

func sortbloom() {

	logger.Printf("starting sortbloom")
	var cmds []*exec.Cmd

	for k := range config.Windows {
		f := fmt.Sprintf("bmatch_%d.txt.sz", k)
		fname := path.Join(tmpdir, f)
		pname1 := pipefromsz(fname)

		cmd2 := exec.Command("sort", "-S", "2G", "--parallel=8", "-k1", pname1)
		cmd2.Env = os.Environ()
		cmd2.Stderr = os.Stderr

		f = fmt.Sprintf("smatch_%d.txt.sz", k)
		fname = path.Join(tmpdir, f)
		cmd3 := exec.Command("sztool", "-c", "-", fname)
		cmd3.Env = os.Environ()
		cmd3.Stderr = os.Stderr
		var err error
		cmd3.Stdin, err = cmd2.StdoutPipe()
		if err != nil {
			panic(err)
		}

		cmds = append(cmds, cmd2, cmd3)
	}

	for _, cmd := range cmds {
		err := cmd.Start()
		if err != nil {
			panic(err)
		}
	}
	for _, cmd := range cmds {
		err := cmd.Wait()
		if err != nil {
			panic(err)
		}
	}

	logger.Printf("sortbloom done")
}

func mergebloom() {
	logger.Printf("starting mergebloom")
	fp := 0
	for {
		nproc := config.MaxMergeProcs
		if nproc > len(config.Windows)-fp {
			nproc = len(config.Windows) - fp
		}
		if nproc == 0 {
			break
		}

		var cmds []*exec.Cmd
		for k := fp; k < fp+nproc; k++ {
			cmd := exec.Command("merge_bloom", tmpjsonfile, fmt.Sprintf("%d", k), tmpdir)
			cmd.Env = os.Environ()
			cmd.Stderr = os.Stderr
			err := cmd.Start()
			if err != nil {
				panic(err)
			}
			cmds = append(cmds, cmd)
		}

		for _, cmd := range cmds {
			err := cmd.Wait()
			if err != nil {
				panic(err)
			}
		}
		fp += nproc
	}
	logger.Printf("mergebloom done")
}

func combinewindows() {

	logger.Printf("starting combinewindows")

	var cmds []*exec.Cmd

	// Pipe everything into one sort/unique
	c0 := exec.Command("sort", "-u", "-")
	c0.Env = os.Environ()
	c0.Stderr = os.Stderr

	// The sorted results go to disk
	outname := path.Join(tmpdir, "matches.txt.sz")
	c1 := exec.Command("sztool", "-c", "-", outname)
	c1.Env = os.Environ()
	c1.Stderr = os.Stderr
	var err error
	c1.Stdin, err = c0.StdoutPipe()
	if err != nil {
		panic(err)
	}

	cmds = append(cmds, c0, c1)

	var fd []io.Reader
	for j := 0; j < len(config.Windows); j++ {
		f := fmt.Sprintf("rmatch_%d.txt.sz", j)
		fname := path.Join(tmpdir, f)
		c := exec.Command("sztool", "-d", fname)
		c.Env = os.Environ()
		c.Stderr = os.Stderr
		cmds = append(cmds, c)
		p, err := c.StdoutPipe()
		if err != nil {
			panic(err)
		}
		fd = append(fd, p)
	}
	c0.Stdin = io.MultiReader(fd...)

	for _, c := range cmds {
		err := c.Start()
		if err != nil {
			panic(err)
		}
	}

	for _, c := range cmds {
		err := c.Wait()
		if err != nil {
			panic(err)
		}
	}

	logger.Printf("combinewindows done")
}

func sortbygeneid() {

	logger.Printf("starting sortbygeneid")
	inname := path.Join(tmpdir, "matches.txt.sz")
	outname := path.Join(tmpdir, "matches_sg.txt.sz")

	// Sort by gene number
	cmd1 := exec.Command("sztool", "-d", inname)
	cmd1.Env = os.Environ()
	cmd1.Stderr = os.Stderr
	cmd2 := exec.Command("sort", "-k 5", "-")
	cmd2.Env = os.Environ()
	cmd2.Stderr = os.Stderr
	var err error
	cmd2.Stdin, err = cmd1.StdoutPipe()
	if err != nil {
		panic(err)
	}
	cmd3 := exec.Command("sztool", "-c", "-", outname)
	cmd3.Env = os.Environ()
	cmd3.Stderr = os.Stderr
	cmd3.Stdin, err = cmd2.StdoutPipe()
	if err != nil {
		panic(err)
	}

	cmds := []*exec.Cmd{cmd1, cmd2, cmd3}
	for _, c := range cmds {
		err := c.Start()
		if err != nil {
			panic(err)
		}
	}
	for _, c := range cmds {
		err := c.Wait()
		if err != nil {
			panic(err)
		}
	}

	logger.Printf("sortbygeneid done")
}

func joingenenames() {

	logger.Printf("starting joingenenames")

	inname := path.Join(tmpdir, "matches_sg.txt.sz")
	pname1 := pipefromsz(inname)
	pname2 := pipefromsz(config.GeneIdFileName)

	cmd1 := exec.Command("join", pname1, pname2, "-1", "5", "-2", "1")
	cmd1.Env = os.Environ()
	cmd1.Stderr = os.Stderr

	// Remove the internal sequence id
	cmd2 := exec.Command("cut", "-d ", "-f", "2-6", "-")
	cmd2.Env = os.Environ()
	cmd2.Stderr = os.Stderr
	pi, err := cmd1.StdoutPipe()
	cmd2.Stdin = pi

	outname := fmt.Sprintf("_%.0f_matches.txt", 100*config.PMatch)
	outname = strings.Replace(config.ReadFileName, ".fastq", outname, 1)
	fid, err := os.Create(outname)
	if err != nil {
		panic(err)
	}
	defer fid.Close()
	cmd2.Stdout = fid

	cmds := []*exec.Cmd{cmd1, cmd2}

	for _, c := range cmds {
		err := c.Start()
		if err != nil {
			panic(err)
		}
	}

	for _, c := range cmds {
		err := c.Wait()
		if err != nil {
			panic(err)
		}
	}

	logger.Printf("joingenenames done")
}

func setupLog() {
	logname := path.Join(tmpdir, "run.log")
	fid, err := os.Create(logname)
	if err != nil {
		panic(err)
	}
	logger = log.New(fid, "", log.Lshortfile)
}

func copyconfig(config *utils.Config, tmpdir string) {

	fid, err := os.Create(path.Join(tmpdir, "config.json"))
	if err != nil {
		panic(err)
	}
	defer fid.Close()
	enc := json.NewEncoder(fid)
	err = enc.Encode(config)
	if err != nil {
		panic(err)
	}
	tmpjsonfile = path.Join(tmpdir, "config.json")
}

func main() {

	if len(os.Args) != 3 {
		panic("wrong number of arguments")
	}

	err := os.Setenv("LC_ALL", "C")
	if err != nil {
		panic(err)
	}
	home := os.Getenv("HOME")
	gopath := path.Join(home, "go")
	err = os.Setenv("GOPATH", gopath)
	if err != nil {
		panic(err)
	}
	err = os.Setenv("PATH", os.Getenv("PATH")+":"+home+"/go/bin")
	if err != nil {
		panic(err)
	}

	jsonfile = os.Args[1]
	config = utils.ReadConfig(jsonfile)

	startpoint, err := strconv.Atoi(os.Args[2])
	if err != nil {
		print("can't determine starting point")
		panic(err)
	}

	if !strings.HasSuffix(config.ReadFileName, ".fastq") {
		panic("Invalid read file")
	}

	// Create the directory for all temporary files, if needed
	var d string
	d, basename = path.Split(config.ReadFileName)
	if config.TempDir == "" {
		d = path.Join(d, "tmp")
		err = os.MkdirAll(d, 0755)
		if err != nil {
			panic(err)
		}
		tmpdir, err = ioutil.TempDir(d, "")
		if err != nil {
			panic(err)
		}
	} else {
		tmpdir = config.TempDir
		err = os.MkdirAll(tmpdir, 0755)
		if err != nil {
			panic(err)
		}
	}
	copyconfig(config, tmpdir)

	setupLog()

	logger.Printf("Storing temporary files in %s", tmpdir)
	pipedir = path.Join(tmpdir, "pipes")
	logger.Printf("Storing pipes in %s", pipedir)
	err = os.MkdirAll(pipedir, 0755)
	if err != nil {
		panic(err)
	}

	if startpoint <= 0 {
		compresssource()
	}

	if startpoint <= 1 {
		sortsource()
	}

	if startpoint <= 2 {
		windowreads()
	}

	if startpoint <= 3 {
		sortwindows()
	}

	if startpoint <= 4 {
		bloom()
	}

	if startpoint <= 5 {
		sortbloom()
	}

	if startpoint <= 6 {
		mergebloom()
	}

	if startpoint <= 7 {
		combinewindows()
	}

	if startpoint <= 8 {
		sortbygeneid()
	}

	if startpoint <= 9 {
		joingenenames()
	}

	logger.Printf("All done, exiting")
}
