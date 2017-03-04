package main

import (
	"fmt"
	"os"
	"os/exec"
	"strings"

	"github.com/kshedden/seqmatch/utils"
)

const ()

var (
	jsonfile string
	config   *utils.Config
)

func compresssource() {

	cmd := exec.Command("go", "run", "compress_source.go", jsonfile)
	cmd.Env = os.Environ()
	cmd.Stderr = os.Stderr
	err := cmd.Run()
	if err != nil {
		panic(err)
	}
}

func sortsource() {

	fname := strings.Replace(config.ReadFileName, ".fastq", ".txt.sz", 1)
	cmd1 := exec.Command("sztool", "-d", fname)
	cmd1.Env = os.Environ()
	cmd1.Stderr = os.Stderr

	cmd2 := exec.Command("sort", "-S", "2G", "--parallel=8")
	cmd2.Env = os.Environ()
	cmd2.Stderr = os.Stderr
	var err error
	cmd2.Stdin, err = cmd1.StdoutPipe()
	if err != nil {
		panic(err)
	}

	cmd3 := exec.Command("uniq", "-c")
	cmd3.Env = os.Environ()
	cmd3.Stderr = os.Stderr
	cmd3.Stdin, err = cmd2.StdoutPipe()
	if err != nil {
		panic(err)
	}

	outname := strings.Replace(config.ReadFileName, ".fastq", "_sorted.txt.sz", 1)
	cmd4 := exec.Command("sztool", "-c", "-", outname)
	cmd4.Env = os.Environ()
	cmd4.Stderr = os.Stderr
	cmd4.Stdin, err = cmd3.StdoutPipe()
	if err != nil {
		panic(err)
	}

	err = cmd1.Start()
	if err != nil {
		panic(err)
	}

	err = cmd2.Start()
	if err != nil {
		panic(err)
	}

	err = cmd3.Start()
	if err != nil {
		panic(err)
	}

	err = cmd4.Run()
	if err != nil {
		panic(err)
	}

	err = cmd3.Wait()
	if err != nil {
		panic(err)
	}

	err = cmd2.Wait()
	if err != nil {
		panic(err)
	}

	err = cmd1.Wait()
	if err != nil {
		panic(err)
	}
}

func windowreads() {
	cmd := exec.Command("go", "run", "window_reads.go", jsonfile)
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
		fname := strings.Replace(config.ReadFileName, ".fastq", "", 1)
		fname = fname + fmt.Sprintf("_win_%d_%d.txt.sz", q1, q2)
		cmd1 := exec.Command("sztool", "-d", fname)
		cmd1.Env = os.Environ()
		cmd1.Stderr = os.Stderr

		cmd2 := exec.Command("sort", "-S", "2G", "--parallel=8", "-k1")
		cmd2.Env = os.Environ()
		cmd2.Stderr = os.Stderr
		var err error
		cmd2.Stdin, err = cmd1.StdoutPipe()
		if err != nil {
			panic(err)
		}

		fname = strings.Replace(fname, ".txt.sz", "_sorted.txt.sz", 1)
		cmd3 := exec.Command("sztool", "-c", "-", fname)
		cmd3.Env = os.Environ()
		cmd3.Stderr = os.Stderr
		cmd3.Stdin, err = cmd2.StdoutPipe()
		if err != nil {
			panic(err)
		}

		err = cmd1.Start()
		if err != nil {
			panic(err)
		}
		err = cmd2.Start()
		if err != nil {
			panic(err)
		}
		err = cmd3.Start()
		if err != nil {
			panic(err)
		}
		cmds = append(cmds, cmd3)
	}

	for _, cmd := range cmds {
		cmd.Wait()
	}
}

func bloom() {
	cmd := exec.Command("go", "run", "bloom.go", jsonfile)
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
		s := fmt.Sprintf("_%d_%d_bmatch.txt.sz", q1, q2)
		fname := strings.Replace(config.ReadFileName, ".fastq", s, 1)
		cmd1 := exec.Command("sztool", "-d", fname)
		cmd1.Env = os.Environ()
		cmd1.Stderr = os.Stderr

		cmd2 := exec.Command("sort", "-S", "2G", "--parallel=8", "-k1")
		cmd2.Env = os.Environ()
		cmd2.Stderr = os.Stderr
		var err error
		cmd2.Stdin, err = cmd1.StdoutPipe()
		if err != nil {
			panic(err)
		}

		s = fmt.Sprintf("_%d_%d_smatch.txt.sz", q1, q2)
		fname = strings.Replace(config.ReadFileName, ".fastq", s, 1)
		cmd3 := exec.Command("sztool", "-c", "-", fname)
		cmd3.Env = os.Environ()
		cmd3.Stderr = os.Stderr
		cmd3.Stdin, err = cmd2.StdoutPipe()
		if err != nil {
			panic(err)
		}

		err = cmd1.Start()
		if err != nil {
			panic(err)
		}
		err = cmd2.Start()
		if err != nil {
			panic(err)
		}
		err = cmd3.Start()
		if err != nil {
			panic(err)
		}

		cmds = append(cmds, cmd1, cmd2, cmd3)
	}

	for _, cmd := range cmds {
		cmd.Wait()
	}
}

func mergebloom() {
	var cmds []*exec.Cmd
	for k, _ := range config.Windows {
		cmd := exec.Command("go", "run", "merge_bloom.go", fmt.Sprintf("%d", k))
		cmd.Env = os.Environ()
		cmd.Stderr = os.Stderr
		err := cmd.Start()
		if err != nil {
			panic(err)
		}
		cmds = append(cmds, cmd)
	}

	for _, cmd := range cmds {
		cmd.Wait()
	}
}

func sortmerge() {

	var cmds []*exec.Cmd

	for _, q1 := range config.Windows {
		q2 := q1 + config.WindowWidth
		s := fmt.Sprintf("_win_%d_%d_%.0f_rmatch.txt", q1, q2, 100*config.PMiss)
		fname := strings.Replace(config.ReadFileName, ".fastq", s, 1)
		cmd := exec.Command("sort", "-o", fname, fname)
		cmd.Env = os.Environ()
		cmd.Stderr = os.Stderr
		err := cmd.Start()
		if err != nil {
			panic(err)
		}
		cmds = append(cmds, cmd)
	}

	for _, cmd := range cmds {
		cmd.Wait()
	}
}

func main() {

	if len(os.Args) != 2 {
		panic("wrong number of arguments")
	}

	os.Setenv("LC_ALL", "C")
	os.Setenv("GOPATH", "/home/kshedden/go_projects")
	os.Setenv("PATH", os.Getenv("PATH")+":/home/kshedden/go_projects/bin")

	jsonfile = os.Args[1]
	config = utils.ReadConfig(jsonfile)

	if !strings.HasSuffix(config.ReadFileName, ".fastq") {
		panic("Invalid read file")
	}

	//compresssource()
	//sortsource()
	//windowreads()
	//sortwindows()
	bloom()
	sortbloom()
	//mergebloom()
	//sortmerge()
}
