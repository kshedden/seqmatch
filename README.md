__Quick start__


* Download the
[install.sh](https://raw.githubusercontent.com/kshedden/seqmatch/master/install.sh),
[run.pbs](https://raw.githubusercontent.com/kshedden/seqmatch/master/run.pbs),
[prep_target.pbs](https://raw.githubusercontent.com/kshedden/seqmatch/master/prep_target.pbs),
and
[config.json](https://raw.githubusercontent.com/kshedden/seqmatch/master/config.json)
files.

* At the shell prompt in Flux, type `module load go`, then `/bin/sh
install.sh`.

* Edit the -M and -A parameters in the `prep_target.pbs` and `run.pbs`
  scripts to contain your email address and Flux account.

* Edit the file name in `prep_target.pbs` to point to the gene data
  file.  The gene data file must be in one of the following two
  formats: (i) a FASTA file, (ii) a text file, with each line having
  two tab-delimited fields: an ideentifier and a sequence.  Then type
  `qsub prep_target.pbs` and wait for this script to complete before
  proceeding.  Note that this script only needs to be run when the
  gene database changes, it does not utilize the sequencing read files
  at all.  If the gene data file has name `genes.txt`, the output
  files for this script are `genes.txt.sz` and `genes_ids.txt.sz`.
  You will need these file names to pass into the next step, below.

* Edit the `config.json` file to contain the proper paths for the read
  and gene files (the gene file name should be the output file of the
  previous step).  Then adjust the run parameters if desired, and run

  `qsub run.pbs --ConfigFileName=config.json`

  to fully process one read file.  This currently takes around five
  hours (for around 90M distinct reads and 60M distinct genes, using
  20 hashes, and PMatch around 0.9-1).  Alternatively, the parameters
  can be passed using command-line flags, as discussed below.

* The output consists of two files: a fastq file containing
  non-matching reads, and a tab-delimited match file, whose output
  columns are:

1. Read sequence

2. Matching subsequence of a target sequence

3. Position within the target where the read matches (counting from 0)

4. Number of mismatches

5. Target sequence identifier

6. Target sequence length

7. Number of copies of read in read pool

8. Read identifier

__Goal and approach__

The goal is to find all approximate matches from a set of reads into a
gene sequence database.  To make the matching tractable and scalable,
we require at least one window within a read to match exactly.  The
remainder of the read can match to a given level of accuracy
(e.g. 90%).  The location of the exact match window can be varied, and
the results pooled.  Thus the overall query can be stated as:
"approximately find all genes that match a length `w` contiguous
subsequence of a read exactly, and that match the overall read in at
least `p` percent of the positions".  The values of `w` and `p` are
configurable.  Smaller values of either of these parameters will yield
a greater number of more approximate matches, and the running time
will generally be longer.

The approach uses [Bloom
filtering](https://en.wikipedia.org/wiki/Bloom_filter), [rolling
hashes](https://en.wikipedia.org/wiki/Rolling_hash), [external
merging](https://en.wikipedia.org/wiki/External_sorting) and
[concurrent
processing](https://en.wikipedia.org/wiki/Concurrent_computing) to
accomplish this goal with modest memory requirements and reasonable
run-time.  For example, around 6GB of RAM should be sufficient for 100
million reads and 60 million target gene sequences, and the results
will be complete in around 5 hours when using 10 cores.

The user provides a parameter `WindowWidth` (for the width of the
exact matching window), and a list of left endpoints for these
windows.  For example, if the window width is 30 and the left
endpoints are 0, 15, 30, 45, and 60, then the exact match windows are
[0, 30), [15, 45), [30, 60), [45, 75), and [60, 90).  For a read to
match a target, the sequence must match exactly in at least one of
these windows.  The remainder of the sequence needs to match such that
the overall identity between the read and its matching genome sequence
exceeds the value given by the parameter `PMatch`.

__Configurable parameters__

Some parameters can be configured in the `config.json` file, or set
using command-line flags as shown below.

* ReadFileName: A file containing sequencing reads in fastq format.

* GeneFileName: A file containing gene sequences.  This can be
  produced by the `prep_target` script, or by other means.  It is a
  Snappy-compressed text file in which each row contains a gene
  sequence (and nothing else).

* GeneIdFile: A file containing gene id's.  This is produced by the
  `prep_target` script, or can be produced by some other means.  Each
  row contains a gene identifier.  The rows align 1-1 with the rows of
  `GeneFileName`.  The file should be compressed with Snappy.

* WindowWidth: The width of a window that must match exactly.

* Windows: The left edges of windows, one of which must match exactly.

* MinReadLength: Reads shorter than this length are skipped.

* MaxReadLength: Reads longer than this length are truncated.

* MinDinuc: The minimum number of distinct dinucleotides that must be
present in a read (or it is dropped).  This eliminates uninformative
matches that take a lot of space and time to enumerate.

* PMatch: The proportion (between 0 and 1) of bases in a gene sequence
that need to match the read.

* BloomSize: The number of bits in the Bloom filter.  Should be around
  two times greater than `NumHash` times the number of gene sequences.

* NumHash: The number of hashes used in the Bloom filter.

* MaxMatches: The maximum number of mathches returned for each window
in a gene.

* MatchMode: Either `first` or `best`.  If `first`, for each window
sequence, the first `MaxMatches` instances of a read match meeting the
`PMatch` criterion are retained.  If `best`, the best `MaxMatches`
read matches are retained, where "best" is based on having the minimum
number of mismatching positions.

* MaxMergeProcs: The maximum number of merge operations that are
  performed concurrently.

* MMTol: Target sequences matching a read are retained if their number
of mismatches is no more than the lowest number of mismatches (for the
read) plus MMTol, e.g. if MMTol=0, then each read is only matched to
target sequences that have the lowest observed number of mismatches
for that read.

A rule of thumb would be to set `BloomSize` equal to twice the number
of reads times `NumHash`.

Each of these parameter names can be used to provide a value via a
command-line flag.  For example,

```
runmatch --ConfigFileName=config.json --NumHash=30
```

runs the code using the `config.json` parameters, except that the
value of `NumHash` is changed to 30.  Alternatively, all the
parameters can be specified via flags, omitting the `config.json` file
entirely, e.g.

```
runmatch --ReadFileName=reads.fasta --GeneFileName=genes.txt.sz --GeneIdFileName=genenames.txt.sz\
    --Windows=10,20,30 --WindowWidth=20 --BloomSize=4000000000 --NumHash=20 --PMatch=0.9\
    --MinDinuc=5 --MinReadLength=50 --MaxMatches=10 --MaxMergeProcs=3
```

__Logging__

Several log files are written to the workspace directory.  If the
program completes successfully, the file `run.log` will end with a
line that reads "All done, exiting".  If this is not the case, the
program has ended early for some reason.  You can look in some of the
other log files to see if any useful error information is present.

__Next steps__

Count the distinct reads that successfully match into the genes:

```
awk '{print $1}' PRT_NOV_15_02_100_matches.txt | sort -u | wc -l
```

Count the distinct genes that successfully match a read:

```
awk '{print $5}' PRT_NOV_15_02_100_matches.txt | sort -u | wc -l
```

Find all the reads that do not match anything (replace matchfile with
the output file of the matching process, and replace tmpdir with the
temporary directory used to store intermediate results):

```
export LC_ALL=C
rm -f tmp[1-2]
mkfifo tmp1
sztool -d tmpdir/reads_sorted.txt.sz | awk '{print $3}' | sort -u > tmp1 &
mkfifo tmp2
awk '{print $8}' matchfile | sort -u > tmp2 &
comm -23 tmp1 tmp2 > nomatches.txt
rm -f tmp1 tmp2
```

Crude check that a claimed match is correct (checking the read sequence):

```
rm -f tmp3
mkfifo tmp3
sztool -d tmp/PRT_NOV_15_02_sorted.txt.sz > tmp3 &
grep -e $(awk '{print $1}' PRT_NOV_15_02_100_matches.txt | head -n100 | tail -n1) tmp3 > c1
rm -f tmp3
```

Crude check that a claimed match is correct (checking the gene sequence):

```
grep -e $(awk '{print $2}' PRT_NOV_15_02_100_matches.txt | head -n1) ALL_ABFVV_Genes_Derep.txt > c2
```