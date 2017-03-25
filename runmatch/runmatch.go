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
	fname := strings.Replace(basename, ".fastq", ".txt.sz", 1)
	fname = path.Join(tmpdir, fname)
	pname1 := pipefromsz(fname)
	logger.Printf("Reading from %s", fname)

	cmd2 := exec.Command("sort", "-S", "2G", "--parallel=8", pname1)
	cmd2.Env = os.Environ()
	cmd2.Stderr = os.Stderr

	cmd3 := exec.Command("uniq", "-c")
	cmd3.Env = os.Environ()
	cmd3.Stderr = os.Stderr
	var err error
	cmd3.Stdin, err = cmd2.StdoutPipe()
	if err != nil {
		panic(err)
	}

	outname := strings.Replace(basename, ".fastq", "_sorted.txt.sz", 1)
	outname = path.Join(tmpdir, outname)
	logger.Printf("Writing to %s", outname)
	cmd4 := exec.Command("sztool", "-c", "-", outname)
	cmd4.Env = os.Environ()
	cmd4.Stderr = os.Stderr
	cmd4.Stdin, err = cmd3.StdoutPipe()
	if err != nil {
		panic(err)
	}

	cmds := []*exec.Cmd{cmd2, cmd3, cmd4}

	for _, c := range cmds {
		err = c.Start()
		if err != nil {
			panic(err)
		}
	}
	logger.Printf("Started all commands")
	for _, c := range cmds {
		err = c.Wait()
		if err != nil {
			panic(err)
		}
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
		q1 := config.Windows[k]
		q2 := q1 + config.WindowWidth
		_, f := path.Split(config.ReadFileName)
		s := fmt.Sprintf("_win_%d_%d.txt.sz", q1, q2)
		f = strings.Replace(f, ".fastq", s, 1)
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

	for _, q1 := range config.Windows {
		q2 := q1 + config.WindowWidth
		_, f := path.Split(config.ReadFileName)
		s := fmt.Sprintf("_%d_%d_bmatch.txt.sz", q1, q2)
		f = strings.Replace(f, ".fastq", s, 1)
		fname := path.Join(tmpdir, f)
		pname1 := pipefromsz(fname)

		cmd2 := exec.Command("sort", "-S", "2G", "--parallel=8", "-k1", pname1)
		cmd2.Env = os.Environ()
		cmd2.Stderr = os.Stderr

		_, f = path.Split(config.ReadFileName)
		s = fmt.Sprintf("_%d_%d_smatch.txt.sz", q1, q2)
		f = strings.Replace(f, ".fastq", s, 1)
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
	var cmds []*exec.Cmd
	for k := range config.Windows {
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
	_, f := path.Split(config.ReadFileName)
	s := fmt.Sprintf("_%.0f_matches.txt.sz", 100*config.PMatch)
	f = strings.Replace(f, ".fastq", s, 1)
	outname := path.Join(tmpdir, f)
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
		q1 := config.Windows[j]
		q2 := q1 + config.WindowWidth

		_, f := path.Split(config.ReadFileName)
		s := fmt.Sprintf("_%d_%d_%.0f_rmatch.txt.sz", q1, q2, 100*config.PMatch)
		f = strings.Replace(f, ".fastq", s, 1)
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
	_, f := path.Split(config.ReadFileName)
	s := fmt.Sprintf("_%.0f_matches.txt.sz", 100*config.PMatch)
	f = strings.Replace(f, ".fastq", s, 1)
	inname := path.Join(tmpdir, f)

	_, f = path.Split(config.ReadFileName)
	s = fmt.Sprintf("_%.0f_matches_sg.txt.sz", 100*config.PMatch)
	f = strings.Replace(f, ".fastq", s, 1)
	outname := path.Join(tmpdir, f)

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

	_, f := path.Split(config.ReadFileName)
	s := fmt.Sprintf("_%.0f_matches_sg.txt.sz", 100*config.PMatch)
	f = strings.Replace(f, ".fastq", s, 1)
	inname := path.Join(tmpdir, f)
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

	s = fmt.Sprintf("_%.0f_matches.txt", 100*config.PMatch)
	outname := strings.Replace(config.ReadFileName, ".fastq", s, 1)
	fid, err := os.Create(outname)
	if err != nil {
		panic(err)
	}
	defer fid.Close()
	w := bufio.NewWriter(fid)
	pi2, err := cmd2.StdoutPipe()
	if err != nil {
		panic(err)
	}
	go io.Copy(w, pi2)
	defer w.Flush()

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

	os.Setenv("LC_ALL", "C")
	home := os.Getenv("HOME")
	gopath := path.Join(home, "go")
	os.Setenv("GOPATH", gopath)
	os.Setenv("PATH", os.Getenv("PATH")+":"+home+"/go/bin")

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

	setupLog()

	// Create the directory for all temporary files, if needed
	if config.TempDir == "" {
		var d string
		d, basename = path.Split(config.ReadFileName)
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
}
