"""
Licence: CC0.

Example implementation of the rsync delta algorithm without a
rolling hash function.

Instead of using a rolling hash I use the first 8 bytes of each
block as the weak lookup key. For data with high entropy, e.g.
gzipped data this works quite well. It breaks hard for arbitrary
data, so use with care.

I am avoiding the rolling hash because a pure python implementation is
_very_ slow. The implementation below is, while not fast, at least
usable for files up to a few hundred megabytes.

Example usage:

# We have an original file on the server and a changed file on the
# client:
sig = generate_signature('/tmp/original.zip')
delta = generate_delta('/tmp/changed.zip', sig)

# Now the client sends up the deltas over the dial up modem and we
# apply them on the server side:
patch('/tmp/original.zip', '/tmp/new.zip', sig, delta)
"""

import hashlib

BLOCK_SIZE = 2**13

def generate_signature(path, block_size=BLOCK_SIZE):
    signatures = []
    f = open(path)
    i = 0
    while True:
        d = f.read(block_size)
        signatures.append((i, d[:8], hashlib.md5(d).digest()))
        i += len(d)
        if len(d) < block_size:
            break
    return signatures

def generate_delta(path, signature, block_size=BLOCK_SIZE):
    sig_lookup = dict((token, hash) for _, token, hash in signature)
    data = open(path).read()
    i = 0
    start = 0
    deltas = []
    while i < len(data) - 8:
        sig = sig_lookup.get(data[i:i+8])
        if sig is None:
            i += 1
            continue

        local_sig = hashlib.md5(data[i:i + block_size]).digest()
        if sig != local_sig:
            i += 1
            continue

        deltas.append(('data', data[start:i]))
        deltas.append(('use', sig))
        i += block_size
        start = i

    deltas.append(('data', data[start:]))
    return deltas

def patch(in_path, out_path, signature, deltas, block_size=BLOCK_SIZE):
    position_lookup = dict((hash, pos) for pos, _, hash in signature)

    in_data = open(in_path).read()
    out_file = open(out_path, 'w')
    for what, data_or_hash in deltas:
        if what == 'use':
            pos = position_lookup[data_or_hash]
            out_file.write(in_data[pos:pos + block_size])
        if what == 'data':
            out_file.write(data_or_hash)
