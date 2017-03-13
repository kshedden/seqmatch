package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"os/exec"
	"path"
	"strings"
	"time"

	"github.com/kshedden/seqmatch/utils"
	"golang.org/x/sys/unix"
)

const ()

var (
	jsonfile string
	config   *utils.Config
	basename string
	tmpdir   string
	pipedir  string
	logger   *log.Logger
)

func compresssource() {
	logger.Printf("compresssource")
	cmd := exec.Command("prep_reads", jsonfile)
	cmd.Env = os.Environ()
	cmd.Stderr = os.Stderr
	err := cmd.Run()
	if err != nil {
		panic(err)
	}
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
		} else {
			print(fmt.Sprintf("%v\n", err))
		}
	}

	panic("unable to create pipe")
}

func sortsource() {

	logger.Printf("sortsource")
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
	logger.Printf("Finished sortsource")
}

func windowreads() {
	logger.Printf("windowreads")
	cmd := exec.Command("window_reads", jsonfile)
	cmd.Env = os.Environ()
	cmd.Stderr = os.Stderr
	err := cmd.Run()
	if err != nil {
		panic(err)
	}
}

func sortwindows() {

	var cmds []*exec.Cmd

	for k := 0; k < len(config.Windows); k++ {
		q1 := config.Windows[k]
		q2 := q1 + config.WindowWidth
		d, f := path.Split(config.ReadFileName)
		s := fmt.Sprintf("_win_%d_%d.txt.sz", q1, q2)
		f = strings.Replace(f, ".fastq", s, 1)
		fname := path.Join(d, "tmp", f)
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
}

func bloom() {
	cmd := exec.Command("bloom", jsonfile)
	cmd.Env = os.Environ()
	cmd.Stderr = os.Stderr
	err := cmd.Run()
	if err != nil {
		panic(err)
	}
}

func sortbloom() {

	var cmds []*exec.Cmd

	for _, q1 := range config.Windows {
		q2 := q1 + config.WindowWidth
		d, f := path.Split(config.ReadFileName)
		s := fmt.Sprintf("_%d_%d_bmatch.txt.sz", q1, q2)
		f = strings.Replace(f, ".fastq", s, 1)
		fname := path.Join(d, "tmp", f)
		pname1 := pipefromsz(fname)

		cmd2 := exec.Command("sort", "-S", "2G", "--parallel=8", "-k1", pname1)
		cmd2.Env = os.Environ()
		cmd2.Stderr = os.Stderr

		d, f = path.Split(config.ReadFileName)
		s = fmt.Sprintf("_%d_%d_smatch.txt.sz", q1, q2)
		f = strings.Replace(f, ".fastq", s, 1)
		fname = path.Join(d, "tmp", f)
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
}

func mergebloom() {
	var cmds []*exec.Cmd
	for k, _ := range config.Windows {
		cmd := exec.Command("merge_bloom", jsonfile, fmt.Sprintf("%d", k))
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
}

func combinewindows() {

	logger.Printf("starting combinewindows")

	var cmds []*exec.Cmd

	// Pipe everything into one sort/unique
	c0 := exec.Command("sort", "-u", "-")
	c0.Env = os.Environ()
	c0.Stderr = os.Stderr

	// The sorted results go to disk
	d, f := path.Split(config.ReadFileName)
	s := fmt.Sprintf("_%.0f_matches.txt.sz", 100*config.PMatch)
	f = strings.Replace(f, ".fastq", s, 1)
	outname := path.Join(d, "tmp", f)
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

		d, f := path.Split(config.ReadFileName)
		s := fmt.Sprintf("_%d_%d_%.0f_rmatch.txt.sz", q1, q2, 100*config.PMatch)
		f = strings.Replace(f, ".fastq", s, 1)
		fname := path.Join(d, "tmp", f)
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

	logger.Printf("sortbygeneid starting")
	d, f := path.Split(config.ReadFileName)
	s := fmt.Sprintf("_%.0f_matches.txt.sz", 100*config.PMatch)
	f = strings.Replace(f, ".fastq", s, 1)
	inname := path.Join(d, "tmp", f)

	d, f = path.Split(config.ReadFileName)
	s = fmt.Sprintf("_%.0f_matches_sg.txt.sz", 100*config.PMatch)
	f = strings.Replace(f, ".fastq", s, 1)
	outname := path.Join(d, "tmp", f)

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

	logger.Printf("joingenenames starting")

	d, f := path.Split(config.ReadFileName)
	s := fmt.Sprintf("_%.0f_matches_sg.txt", 100*config.PMatch)
	f = strings.Replace(f, ".fastq", s, 1)
	inname := path.Join(d, "tmp", f)
	pname1 := pipefromsz(inname)
	pname2 := pipefromsz(config.GeneIdFileName)

	cmd1 := exec.Command("join", pname1, pname2, "-1", "5", "-2", "1")
	cmd1.Env = os.Environ()
	cmd1.Stderr = os.Stderr

	// Remove the internal sequence id
	cmd2 := exec.Command("cut", "-d", "\" \"", "-f", "2-6", "-")
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

func nonmatching() {

	a1 := fmt.Sprintf("<(sztool -d %s)",
		strings.Replace(config.ReadFileName, ".fastq", "_sorted.txt.sz", 1))
	a2 := fmt.Sprintf("<(cut -k1 %s | sort -u)",
		strings.Replace(config.ReadFileName, ".fastq", "_matches.txt", 1))
	outname := strings.Replace(config.ReadFileName, ".fastq", "_nomatch.txt", 1)
	cmd := exec.Command("comm", "-23", a1, a2, ">", outname)
	cmd.Env = os.Environ()
	cmd.Stderr = os.Stderr
	err := cmd.Run()
	if err != nil {
		panic(err)
	}
}

func setupLog() {
	d, f := path.Split(config.ReadFileName)
	f = strings.Replace(f, ".fastq", "_run.log", 1)
	logname := path.Join(d, "tmp", f)
	fid, err := os.Create(logname)
	if err != nil {
		logger.Print(err)
		panic(err)
	}
	logger = log.New(fid, "", log.Lshortfile)
}

func main() {

	if len(os.Args) != 2 {
		panic("wrong number of arguments")
	}

	os.Setenv("LC_ALL", "C")
	home := os.Getenv("HOME")
	gopath := path.Join(home, "go")
	os.Setenv("GOPATH", gopath)
	os.Setenv("PATH", os.Getenv("PATH")+":"+home+"/go/bin")

	jsonfile = os.Args[1]
	config = utils.ReadConfig(jsonfile)

	if !strings.HasSuffix(config.ReadFileName, ".fastq") {
		panic("Invalid read file")
	}

	setupLog()

	// Create the directory for all temporary files, if needed
	var d string
	d, basename = path.Split(config.ReadFileName)
	tmpdir = path.Join(d, "tmp")
	os.Mkdir(tmpdir, 0755) // ignore the error if the directory exists
	pipedir = path.Join(tmpdir, "pipes")
	os.Mkdir(pipedir, 0755) // ignore the error if the directory exists

	/*compresssource()
	sortsource()
	windowreads()
	sortwindows()
	bloom()
	sortbloom()
	mergebloom()
	combinewindows()
	sortbygeneid()*/
	joingenenames()
	//nonmatching()
}
