Outline of strategy:

1. Restructure the targets so that all letters other than A/T/G/C are
converted to X, and the file has one line per sequence with format
sequence/newline/identifier.

2. Sort and deduplicate the reads, then compress them and write them
to a file with format: sequence<tab>count<tab>newline.  The count is
the number of times each distinct read occurs in the original raw read
pool.  Then compress and restructure the file to have one line per
gene, with format: sequence/tab/identifier. Sequence elements other
than A/T/G/C are converted to X.  Reads that are nearly all A or
nearly all T are dropped.

3. Create a windowed read file with structure: tag<tab>read
sequence<tab>count<tab>newline.  The tag is a subsequence of the full
read beginning and ending at defined locations.

4. Run `bloom.go` to identify candidate matches based on the windowed
reads.  Gene sequence segments that are nearly all A or nearly all T
are skipped.  This step has a small false positive rate and no false
negatives.

5. Sort the candidate matches from step 4.

6. Use a merging procedure to exclude candidate matches that are not
true matches.  Directly check the sequence flanking the window to be
sure that the full read matches the gene sequence (within a given
tolerance of mismatching sites).  The output file has the format: read
sequence<tab>position<tab>weight</tab>gene id.  The position is the
offset within the gene sequence where the read matches.  The weight is
the number of exact copies of the read in the original sequencing
pool.
