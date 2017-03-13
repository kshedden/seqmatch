__Quick start__


* Download the `install.sh`, `run.pbs`, `prep_target.pbs`, and
`config.json` files from [here](https://github.com/kshedden/seqmatch).


* At the shell prompt in Flux, type `module load go`, then `/bin/sh
install.sh`.

* Edit the file name in `prep_target.pbs` to point to the gene data
  file.  Then type `qsub prep_target.pbs` and wait for this script to
  complete before proceeding (it takes around one hour).  Note that
  this script only needs to be run when the gene database changes, it
  does not utilize the sequencing read files at all.

* Edit the `config.json` file to contain the proper paths for the read
  and gene files, and adjust the run parameters if desired.  Then run
  `qsub run.pbs` to fully process one read file.

__Goal and approach__

The goal is to find all approximate matches from a set of reads into a
target sequence database.  To make the matching tractable and
scalable, we require a window within the read to match perfectly.  The
remainder of the read can match to a given level of accuracy
(e.g. 90%).  Results for multiple exact match windows can be
calculated in parallel and pooled.

The approach uses [Bloom
filtering](https://en.wikipedia.org/wiki/Bloom_filter), [rolling
hashes](https://en.wikipedia.org/wiki/Rolling_hash), and [external
merging](https://en.wikipedia.org/wiki/External_sorting) to accomplish
this goal with modest memory requirements and reasonable run-time.
For example, around 6GB of RAM should be sufficient for 100 million
reads and 60 million target gene sequences, and the results will be
complete in around 5 hours when using 10 cores.

The user provides a window width (for the exact matching window), and
a list of left endpoints for these windows.  For example, if the
window width is 30 and the left endpoints are 0, 15, 30, 45, and 60,
then the windows are [0, 30), [15, 45), [30, 60), [45, 75), and [60,
90).  For a read to match a target, the sequence must match exactly in
at least one of these windows.  The remainder of the sequence needs to
match such that the overall identity between the read and its matching
genome sequence is the value given by the parameter `PMatch`.

