/*
This is the entry point for all the scripts in this collection.
Normally, this is the only script that will be run directly.  It calls
the other scripts in turn.
*/

package main

import (
	"bufio"
	"encoding/json"
	"flag"
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

var (
	jsonfile    string
	startpoint  int
	tmpjsonfile string
	config      *utils.Config
	basename    string
	tmpdir      string
	pipedir     string
	logger      *log.Logger
)

func pipename() string {
	f := fmt.Sprintf("%09d", rand.Int63()%1e9)
	return path.Join(pipedir, f)
}

// pipefromsz creates a fifo and starts decompressing the given snappy
// file into it.
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

	cmd0 := exec.Command("prep_reads", tmpjsonfile, tmpdir)
	cmd0.Env = os.Environ()
	cmd0.Stderr = os.Stderr

	cmd1 := exec.Command("sort", "-S", "2G", "--parallel=8")
	cmd1.Env = os.Environ()
	cmd1.Stderr = os.Stderr
	var err error
	cmd1.Stdin, err = cmd0.StdoutPipe()
	if err != nil {
		panic(err)
	}
	pip, err := cmd1.StdoutPipe()
	if err != nil {
		panic(err)
	}

	cmds := []*exec.Cmd{cmd0, cmd1}

	for _, cmd := range cmds {
		err = cmd.Start()
		if err != nil {
			panic(err)
		}
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
	name := []string{fields[1]}
	n := 1
	nseq := 0

	dowrite := func(seq string, name []string, n int) {
		xn := strings.Join(name, ";")
		if len(xn) > 1000 {
			xn = xn[0:995]
			xn += "..."
		}
		nseq++
		_, err = wtr.Write([]byte(seq))
		if err != nil {
			panic(err)
		}
		_, err = wtr.Write([]byte("\t"))
		if err != nil {
			panic(err)
		}
		s := fmt.Sprintf("%d\t%s\n", n, xn)
		_, err = wtr.Write([]byte(s))
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
			name = append(name, name1)
			continue
		}

		dowrite(seq, name, n)
		seq = seq1
		name = name[0:1]
		name[0] = name1
		n = 1
	}

	dowrite(seq, name, n)

	if err := scanner.Err(); err != nil {
		panic(err)
	}

	for _, cmd := range cmds {
		if err := cmd.Wait(); err != nil {
			log.Fatal(err)
		}
	}

	logger.Printf(fmt.Sprintf("Wrote %d read sequences", nseq))
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

	for k := 0; k < len(config.Windows); k++ {
		f := fmt.Sprintf("win_%d.txt.sz", k)
		fname := path.Join(tmpdir, f)
		pname1 := pipefromsz(fname)

		cmd1 := exec.Command("sort", "-S", "2G", "--parallel=8", "-k1", pname1)
		cmd1.Env = os.Environ()
		cmd1.Stderr = os.Stderr

		fname = strings.Replace(fname, ".txt.sz", "_sorted.txt.sz", 1)
		cmd2 := exec.Command("sztool", "-c", "-", fname)
		cmd2.Env = os.Environ()
		cmd2.Stderr = os.Stderr
		var err error
		cmd2.Stdin, err = cmd1.StdoutPipe()
		if err != nil {
			panic(err)
		}

		cmds := []*exec.Cmd{cmd1, cmd2}

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

	for k := range config.Windows {
		f := fmt.Sprintf("bmatch_%d.txt.sz", k)
		fname := path.Join(tmpdir, f)
		pname1 := pipefromsz(fname)

		cmd1 := exec.Command("sort", "-S", "2G", "--parallel=8", "-k1", pname1)
		cmd1.Env = os.Environ()
		cmd1.Stderr = os.Stderr

		f = fmt.Sprintf("smatch_%d.txt.sz", k)
		fname = path.Join(tmpdir, f)
		cmd2 := exec.Command("sztool", "-c", "-", fname)
		cmd2.Env = os.Environ()
		cmd2.Stderr = os.Stderr
		var err error
		cmd2.Stdin, err = cmd1.StdoutPipe()
		if err != nil {
			panic(err)
		}

		cmds := []*exec.Cmd{cmd1, cmd2}

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
			logger.Printf("Starting a round of merge processes")
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

func writebest(lines []string, bfr [][]string, wtr io.Writer, ibuf []int, mmtol int) []int {
	ibuf = ibuf[0:0]
	best := -1
	for _, x := range bfr {
		y, err := strconv.Atoi(x[3]) // 3 is position of nmiss
		if err != nil {
			panic(err)
		}
		if best == -1 || y < best {
			best = y
		}
		ibuf = append(ibuf, y)
	}

	for i, x := range lines {
		if ibuf[i] <= best+mmtol {
			_, err := wtr.Write([]byte(x))
			if err != nil {
				panic(err)
			}
			_, err = wtr.Write([]byte("\n"))
			if err != nil {
				panic(err)
			}
		}
	}

	return ibuf
}

func combinewindows() {

	logger.Printf("starting combinewindows")

	mmtol := config.MMTol

	// Pipe everything into one sort/unique
	c0 := exec.Command("sort", "-S", "2G", "--parallel=8", "-u", "-")
	c0.Env = os.Environ()
	c0.Stderr = os.Stderr
	cmds := []*exec.Cmd{c0}

	// The sorted results go to disk
	outname := path.Join(tmpdir, "matches.txt.sz")
	out, err := os.Create(outname)
	if err != nil {
		panic(err)
	}
	wtr := snappy.NewBufferedWriter(out)

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
	da, err := c0.StdoutPipe()
	if err != nil {
		panic(err)
	}

	for _, c := range cmds {
		err := c.Start()
		if err != nil {
			panic(err)
		}
	}

	// Taking all matches for the same read, retain only those
	// with nmiss equal to at most one greater than the lowest
	// nmiss.
	sem := make(chan bool, 1)
	sem <- true
	go func() {
		scanner := bufio.NewScanner(da)
		var lines []string
		var fields [][]string
		var ibuf []int
		var current string
		for scanner.Scan() {
			line := scanner.Text()
			field := strings.Fields(line)

			// Add to the current block.
			if current == "" || field[0] == current {
				lines = append(lines, line)
				fields = append(fields, field)
				current = field[0]
				continue
			}

			// Process a block
			ibuf = writebest(lines, fields, wtr, ibuf, mmtol)
			lines[0] = line
			fields[0] = field
			lines = lines[0:1]
			fields = fields[0:1]
			current = field[0]
		}
		// Process the final block
		writebest(lines, fields, wtr, ibuf, mmtol)
		<-sem
	}()

	for _, c := range cmds {
		err := c.Wait()
		if err != nil {
			panic(err)
		}
	}
	sem <- true

	wtr.Close()
	out.Close()

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
	// k5 is position of gene id
	cmd2 := exec.Command("sort", "-S", "2G", "--parallel=8", "-k5", "-")
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
	cmd2 := exec.Command("cut", "-d ", "-f", "1", "--complement", "-")
	cmd2.Env = os.Environ()
	cmd2.Stderr = os.Stderr
	pi, err := cmd1.StdoutPipe()
	cmd2.Stdin = pi

	// Output file
	outname := path.Join(tmpdir, "matches_sn.txt.sz")
	fid, err := os.Create(outname)
	if err != nil {
		panic(err)
	}
	defer fid.Close()
	wtr := snappy.NewBufferedWriter(fid)
	defer wtr.Close()
	cmd2.Stdout = wtr

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

func joinreadnames() {

	logger.Printf("starting joinreadnames")

	inname := path.Join(tmpdir, "matches_sn.txt.sz")
	pnamem := pipefromsz(inname)

	rfname := path.Join(tmpdir, "reads_sorted.txt.sz")
	pnamer := pipefromsz(rfname)

	// Pipe to accept the sorted matches
	name := pipename()
	err := unix.Mkfifo(name, 0755)
	if err != nil {
		panic(err)
	}

	// Sort the matches
	cmd1 := exec.Command("sort", "-S", "2G", "--parallel=8", "-k1", pnamem)
	cmd1.Env = os.Environ()
	cmd1.Stderr = os.Stderr
	fif, err := os.OpenFile(name, os.O_RDWR, 0600)
	cmd1.Stdout = fif
	if err != nil {
		panic(err)
	}

	// Output file
	_, outname := path.Split(config.ReadFileName)
	s := fmt.Sprintf("_%.0f_%d_%d_matches.txt", 100*config.PMatch, len(config.Windows), config.WindowWidth)
	outname = strings.Replace(outname, ".fastq", s, 1)
	fid, err := os.Create(outname)
	if err != nil {
		panic(err)
	}
	defer fid.Close()
	wtr := bufio.NewWriter(fid)
	defer wtr.Flush()

	cmd2 := exec.Command("join", name, pnamer, "-1", "1", "-2", "1")
	cmd2.Env = os.Environ()
	cmd2.Stderr = os.Stderr
	cmd2.Stdout = wtr

	cmds := []*exec.Cmd{cmd1, cmd2}

	for _, c := range cmds {
		err := c.Start()
		if err != nil {
			panic(err)
		}
	}

	err = cmd1.Wait()
	if err != nil {
		panic(err)
	}
	fif.Close()

	err = cmd2.Wait()
	if err != nil {
		panic(err)
	}

	logger.Printf("joinreadnames done")
}

func setupLog() {
	logname := path.Join(tmpdir, "run.log")
	fid, err := os.Create(logname)
	if err != nil {
		panic(err)
	}
	logger = log.New(fid, "", log.Ltime)
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

func handleArgs() {

	ConfigFileName := flag.String("ConfigFileName", "", "JSON file containing configuration parameters")
	ReadFileName := flag.String("ReadFileName", "", "Sequencing read file (fastq format)")
	GeneFileName := flag.String("GeneFileName", "", "Gene file name (processed form)")
	GeneIdFileName := flag.String("GeneIdFileName", "", "Gene ID file name (processed form)")
	WindowsRaw := flag.String("Windows", "", "Starting position of each window")
	WindowWidth := flag.Int("WindowWidth", 0, "Width of each window")
	BloomSize := flag.Int("BloomSize", 0, "Size of Bloom filter, in bits")
	NumHash := flag.Int("NumHash", 0, "Number of hashses")
	PMatch := flag.Float64("PMatch", 0, "Required proportion of matching positions")
	MinDinuc := flag.Int("MinDinuc", 0, "Minimum number of dinucleotides to check for match")
	TempDir := flag.String("TempDir", "", "Workspace for temporary files")
	MinReadLength := flag.Int("MinReadLength", 0, "Reads shorter than this length are skipped")
	MaxReadLength := flag.Int("MaxReadLength", 0, "Reads longer than this length are truncated")
	MaxMatches := flag.Int("MaxMatches", 0, "Return no more than this number of matches per window")
	MaxMergeProcs := flag.Int("MaxMergeProcs", 0, "Run this number of merge processes concurrently")
	StartPoint := flag.Int("StartPoint", 0, "Restart at a given point in the procedure")
	MatchMode := flag.String("MatchMode", "", "'first' (retain first matches meeting criteria) or 'best' (returns best matches meeting criteria)")

	flag.Parse()

	if *ConfigFileName != "" {
		jsonfile = *ConfigFileName
		config = utils.ReadConfig(jsonfile)
	} else {
		config = new(utils.Config)
	}

	if *ReadFileName != "" {
		config.ReadFileName = *ReadFileName
	}
	if *GeneFileName != "" {
		config.GeneFileName = *GeneFileName
	}
	if *GeneIdFileName != "" {
		config.GeneIdFileName = *GeneIdFileName
	}
	if *WindowWidth != 0 {
		config.WindowWidth = *WindowWidth
	}
	if *BloomSize != 0 {
		config.BloomSize = uint64(*BloomSize)
	}
	if *NumHash != 0 {
		config.NumHash = *NumHash
	}
	if *PMatch != 0 {
		config.PMatch = *PMatch
	}
	if *MinDinuc != 0 {
		config.MinDinuc = *MinDinuc
	}
	if *TempDir != "" {
		config.TempDir = *TempDir
	}
	if *MinReadLength != 0 {
		config.MinReadLength = *MinReadLength
	}
	if *MaxReadLength != 0 {
		config.MaxReadLength = *MaxReadLength
	}
	if *MaxMatches != 0 {
		config.MaxMatches = *MaxMatches
	}
	if *MaxMergeProcs != 0 {
		config.MaxMergeProcs = *MaxMergeProcs
	}
	if *MatchMode != "" {
		config.MatchMode = *MatchMode
	}

	startpoint = *StartPoint

	if *WindowsRaw != "" {
		toks := strings.Split(*WindowsRaw, ",")
		var itoks []int
		for _, x := range toks {
			y, err := strconv.Atoi(x)
			if err != nil {
				panic(err)
			}
			itoks = append(itoks, y)
		}
		config.Windows = itoks
	}
}

func checkArgs() {

	if config.ReadFileName == "" {
		os.Stderr.WriteString("ReadFileName not provided\n")
		os.Exit(1)
	}
	if config.GeneFileName == "" {
		os.Stderr.WriteString("GeneFileName not provided\n")
		os.Exit(1)
	}
	if config.GeneIdFileName == "" {
		os.Stderr.WriteString("GeneIdFileName not provided\n")
		os.Exit(1)
	}
	if len(config.Windows) == 0 {
		os.Stderr.WriteString("Windows not provided\n")
		os.Exit(1)
	}
	if config.WindowWidth == 0 {
		os.Stderr.WriteString("WindowWidth not provided\n")
		os.Exit(1)
	}
	if config.BloomSize == 0 {
		os.Stderr.WriteString("BloomSize not provided\n")
		os.Exit(1)
	}
	if config.NumHash == 0 {
		os.Stderr.WriteString("NumHash not provided\n")
		os.Exit(1)
	}
	if config.PMatch == 0 {
		os.Stderr.WriteString("PMatch not provided\n")
		os.Exit(1)
	}
	if config.MaxReadLength == 0 {
		os.Stderr.WriteString("MaxReadLength not provided\n")
		os.Exit(1)
	}
	if config.MaxMatches == 0 {
		os.Stderr.WriteString("MaxMatches not provided\n")
		os.Exit(1)
	}
	if config.MaxMergeProcs == 0 {
		os.Stderr.WriteString("MaxMergeProcs not provided, defaulting to 3\n")
		config.MaxMergeProcs = 3
	}
	if !strings.HasSuffix(config.ReadFileName, ".fastq") {
		msg := fmt.Sprintf("Warning: %s may not be a fastq file, continuing anyway\n",
			config.ReadFileName)
		os.Stderr.WriteString(msg)
	}
	if config.MatchMode == "" {
		os.Stderr.WriteString("MatchMode not provided, defaulting to 'first'\n")
		config.MatchMode = "first"
	}
}

func setupEnvs() {
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
}

// Create the directory for all temporary files, if needed
func makeTemp() {
	var d string
	var err error
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

	pipedir = path.Join(tmpdir, "pipes")
	err = os.MkdirAll(pipedir, 0755)
	if err != nil {
		panic(err)
	}
}

func run() {
	if startpoint <= 0 {
		sortsource()
	}

	if startpoint <= 1 {
		windowreads()
	}

	if startpoint <= 2 {
		sortwindows()
	}

	if startpoint <= 3 {
		bloom()
	}

	if startpoint <= 4 {
		sortbloom()
	}

	if startpoint <= 5 {
		mergebloom()
	}

	if startpoint <= 6 {
		combinewindows()
	}

	if startpoint <= 7 {
		sortbygeneid()
	}

	if startpoint <= 8 {
		joingenenames()
	}

	if startpoint <= 9 {
		joinreadnames()
	}
}

func main() {

	handleArgs()
	checkArgs()
	setupEnvs()
	makeTemp()
	copyconfig(config, tmpdir)
	setupLog()

	logger.Printf("Storing temporary files in %s", tmpdir)

	run()

	logger.Printf("All done, exiting")
}
