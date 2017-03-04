__Goal and approach__

The goal is to find all approximate matches from a set of reads into a
target sequence database.  To make the matching tractable and
scalable, we require a window within the read to match perfectly.  The
remainder of the read can match to a given level of accuracy
(e.g. 90%).  Results for multiple window positions can be calculated
in parallel and pooled.

The approach uses Bloom filtering, running hashes, and external
merging to accomplish this goal with modest memory requirements and
reasonable run-time.  For example, around 6GB of RAM should be
sufficient for 100 million reads and 60 million target gene sequences,
and the results will be complete in around 5 hours when using 10
cores.

The user provides a window width (for the exact matching window), and
a list of left endpoint for these windows.  For example, if the window
width is 30 and the left endpoints are 0, 15, 30, 45, and 60, then the
windows are [0, 30), [15, 45), [30, 60), [45, 75), and [60, 90).  For
a read to match a target, the sequence must match exactly in at least
one of these windows.  The remainder of the sequence only needs to
match so that the overall non-identity between the read and its
mathing genome sequence is the value given by the parameter `PMiss`.

__Using the tools__

TODO: provide an install script that sets up the environment

1. Build the target sequence database in the format required by the
tool.  The pbs script "builddb.pbs" accomplishes this.  Edit the last
line of builddb.pbs to contain the full path to the target sequence
data file, which must be an uncompressed text file in which each line
has the format "id<tab>sequence<newline>".  This may take a few hours.

2. Edit the config.json file to contain the paths to the read, gene
target, and gene id data.  Then run the run.pbs script to do all the
work for one read file.

__Outline of strategy__

1. Sort and deduplicate the reads, then compress them and write them
to a file with format: `sequence<tab>count<newline>`.  The value of
`count` is the number of times each distinct read occurs in the
original raw read pool.

2. Create windowed read files with structure: `window
sequence<tab>left sequence<tab>right sequence>count<newline>`.  The
tag sequence is a subsequence of the full read, beginning and ending
at defined locations.  The left and right sequence comprise all
remaining positions in the read that are not in the window.  Reads
with insufficient dinucleotide diversity or that are too short to
cover the window are skipped.

3. Run `bloom.go` to identify candidate matches in the target
sequences to the windowed reads.  This step has a small false positive
rate and no false negatives.

4. Sort the candidate matches from step 4.

5. Use a merging procedure to exclude candidate matches from the Bloom
filter that are not true matches.  Directly check the sequence
flanking the window to be sure that the full read matches the gene
sequence to the desired tolerance.  The output file has the format:
`read sequence<tab>position<tab>weight</tab>gene id`.  The value of
`position` is the offset within the gene sequence where the read
matches.  The value of `weight` is the number of exact copies of the
read in the original sequencing pool.
