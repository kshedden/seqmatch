1. Run compress.go to convert the raw sequence and target files to
gzip compressed files.  In these files, one sequence is placed on each
line, with the structure: <sequence id><tab><sequence><newline>.
Sequence elements other than A/T/G/C are converted to X.

2. Run genseqdb.go separately for sources and targets to generate two
leveldb databases of the sequences.

(steps 1-2 above are not significant in overall run-time)

3. Run genhash.go to generate a bunch of hashes, saving only the
tails.  Each tail tells us that ~20K source sequences can only match
into a common set of ~60K target sequences.  Currently we generate 100
hashes per run but we need more, perhaps 500 (counts as 1000 because
we use min and max).  Currently this is much too slow (need to profile
and find a better hashing strategy).  Aim to get this to under 24
hours per 1000 hashes.  Then each run would cover around 20 million
source sequences (20K/hash * 1K hashes).

4. Run prochash.go to do brute-force sequence comparisons of
everything found in step 3.  We can do 400K sequence comparisons per
second on one core, and for each hash we need to do 20K * 60K
comparisons.  This would require around 1 hour to do all the
comparisons for one hash.  With 20 cores, this would be 2 days.
Perhaps something other than brute-force could be used here, but
brute-force is feasible at this scale.

5. Iterate 3/4 until all the sequences are processed.  With the
numbers above, we would need to run ~10 times to cover the whole
dataset.  The whole process works out to around one month with 20
cores.

Things to adjust/optimize:

* k-mer width, currently using k=20

* How much of the tail to save?  More stringent cutoffs at step 3
  leads to less work at step 4, but more iterations at step 5.
