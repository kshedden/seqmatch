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
  file.  Then type `qsub prep_target.pbs` and wait for this script to
  complete before proceeding (it takes under one hour).  Note that
  this script only needs to be run when the gene database changes, it
  does not utilize the sequencing read files at all.

* Edit the `config.json` file to contain the proper paths for the read
  and gene files (the gene file name should be the output file of the
  previous step).  Then adjust the run parameters if desired, and run
  `qsub run.pbs` to fully process one read file.  This currently takes
  around five hours (for around 90M distinct reads and 60M distinct
  genes, using 20 hashes, and PMatch around 0.9-1).

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
hashes](https://en.wikipedia.org/wiki/Rolling_hash), and [external
merging](https://en.wikipedia.org/wiki/External_sorting) to accomplish
this goal with modest memory requirements and reasonable run-time.
For example, around 6GB of RAM should be sufficient for 100 million
reads and 60 million target gene sequences, and the results will be
complete in around 5 hours when using 10 cores.

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

Some parameters can be configured in the `config.json` file:

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

* MaxMergeProcs: The maximum number of merge operations that are
  performed concurrently.

A rule of thumb would be to set `BloomSize` equal to twice the number
of reads times `NumHash`.

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

Find all the reads that do not match anything:

```
export LC_ALL=C
rm -f tmp[1-2]
mkfifo tmp1
sztool -d tmp/PRT_NOV_15_02_sorted.txt.sz | awk '{print $2}' | sort -u > tmp1 &
mkfifo tmp2
awk '{print $1}' PRT_NOV_15_02_100_matches.txt | sort -u > tmp2 &
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