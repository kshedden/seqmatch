Outline of strategy:

1. Restructure the targets so that all letters other than A/T/G/C are
converted to X, and the file has one line per sequence with format
sequence/newline/identifier.

2. Sort and deduplicate the reads, then compress them and write them
to a file with format: sequence/count/newline.  The count is the
number of times eah distinct read occurs in the original raw read
pool.  Then compress and restructure the file to have one line per
gene, with format: sequence/tab/identifier. Sequence elements other
than A/T/G/C are converted to X.

3. Create a windowed read file with structure: tag/tab/read
sequence/tab/count/newline.  The tag is a subsequence of the full read
beginning and ending at defined locations.

4. Run `bloom.go` to identify candidate matches based on the windowed
reads.

5. Sort the candidate matches from step 4.

6. Use a merging procedure to exclude candidate matches that are not
truly matches.
