# Introduction

This is implementation of the rsync delta algorithm without a rolling
hash function.

Instead of using a rolling hash I use the first 8 bytes of each block
as the weak lookup key. For data with high entropy, e.g.  gzipped data
this works quite well. It breaks hard for arbitrary data, so use with
care.

You can check the Shannon entropy of your data with the ent.py script:

```python
$  cat /tmp/data.tgz | python ent.py 
0.999503541539
```

High entropy means something close to 1.

I am avoiding the rolling hash because a pure python implementation is
_very_ slow. The implementation below is, while not fast, at least
usable for files up to a few hundred megabytes.

# Example usage:

```python
from deltadiff2 import *

# We have an original file on the server and a changed file on the
# client:
sig = generate_signature('/tmp/original.zip')
delta = generate_delta('/tmp/changed.zip', sig)

# Now the client sends up the deltas over the dial up modem and we
# apply them on the server side:
patch('/tmp/original.zip', '/tmp/new.zip', sig, delta)
```
