# Linux source local and remote XFS tool comparison

This run extends the sealed run8 Linux 7.2.9 source-tree comparison with serial `cp` and `rsync`.
It uses the same unpacked dataset and reacquired source and peer EC2 instances as run8.

The measured matrix preserves the September and run8 methodology: three repetitions, source
staging onto local XFS outside measured time, client `drop_caches=3`, a five-second settle, GNU
time around only the copy command, destination sync and count/byte validation after timing, and
destination cleanup.

Local XFS runs default rcp, default 32-rank dcp, `cp -a --reflink=auto`, and `rsync -a` three times
each. Physical remote XFS runs default rcp and serial `rsync -a` three times each. No 10-way
variants or explicit rcp concurrency overrides are included.
