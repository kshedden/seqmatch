Outline of strategy:

1. Run `compress.go` to convert the raw sequence and target files to
gzip compressed files.  In these files, one sequence is placed on each
line, with the structure: sequence id/tab/sequence/newline.  Sequence
elements other than A/T/G/C are converted to X.

2. Run `sort_sources.pbs` to sort and de-duplicate the reads,
retaining the count information

3. Run `bloom.go` to identify candidate matches.

4. Run `sort_matches.pbs` to sort the matches

5. Run `merge_matches.go` to merge the matches with the source file.
This step eliminates false positives from the Bloom filter.


Notes:

* The `sw` parameter sets the sequence width.  It should be set to the
  same value in all files.  Reads shorter than this width are skipped.
  Reads that are `sw` or longer in length are only matched based on
  the first `sw` values.

* The output file gives all subsequences from the target genes that
  match the source sequences (truncated to length `sw` as noted
  above).  Each target gene may match multiple source sequences, and
  these source sequences may have weights greater than 1 based on the
  source deduplication (step 2 above).  To infer an expression level
  we should add the weights over all source sequences that match into
  a given target gene.