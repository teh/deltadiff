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
import struct

BLOCK_SIZE = 2**13
BLOCK_USE = 1
BLOCK_DATA = 2

def generate_signature(path, block_size=BLOCK_SIZE):
    signatures = []
    f = open(path)
    i = 0
    while True:
        d = f.read(block_size)
        if len(d) < 8:
            d += '\0'*(8-len(d))
        signatures.append(struct.pack('>I8s16s', i, d, hashlib.md5(d).digest()))
        i += len(d)
        if len(d) < block_size:
            break
    return ''.join(signatures)

def unpack_signature(binary_signature):
    """
    Takes the binary encoded signature and returns 3-tuples
    of offset, token, hash
    """
    assert len(binary_signature) % 28 == 0, "Invalid signature length"
    for i in xrange(len(binary_signature) // 28):
        yield struct.unpack('>I8s16s', binary_signature[i*28:(i+1)*28])

def generate_delta(path, signature, block_size=BLOCK_SIZE):
    sig_lookup = dict(
        (token, hash) for _, token, hash in unpack_signature(signature)
    )
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

def pack_delta(delta):
    binary_delta = []
    for what, data in delta:
        # length prefixed data or signature
        if what == 'data':
            binary_delta.append(struct.pack('>bI', BLOCK_DATA, len(data)) + data)
        if what == 'use':
            binary_delta.append(struct.pack('>bI', BLOCK_USE, len(data)) + data)
    return ''.join(binary_delta)

def unpack_delta(binary_delta):
    i = 0
    while i < len(binary_delta):
        what, data_len = struct.unpack('>bI', binary_delta[i:i+5])
        data = binary_delta[i+5:i+5+data_len]
        assert len(data) == data_len, "Truncated data."
        if what == BLOCK_DATA:
            yield 'data', data
        if what == BLOCK_USE:
            yield 'use', data
        i += 5 + data_len

def patch(in_path, out_path, signature, binary_delta, block_size=BLOCK_SIZE):
    position_lookup = dict((hash, pos) for pos, _, hash in unpack_signature(signature))

    in_data = open(in_path).read()
    out_file = open(out_path, 'w')
    for what, data_or_hash in unpack_delta(binary_delta):
        if what == 'use':
            pos = position_lookup[data_or_hash]
            out_file.write(in_data[pos:pos + block_size])
        if what == 'data':
            out_file.write(data_or_hash)

# Nosetests
import tempfile
def test_sign_empty():
    with tempfile.NamedTemporaryFile() as tf:
        # empty
        sig = generate_signature(tf.name)
        assert len(sig) == 28

def test_sign_full():
    with tempfile.NamedTemporaryFile() as tf:
        tf.write('*'*2**20)
        tf.flush()
        sig = generate_signature(tf.name)
        assert len(sig) == 3612
        
def test_generate_delta():
    with tempfile.NamedTemporaryFile() as tf:
        with open('/dev/urandom') as ur:
            tf.write(ur.read(2**16))
            tf.flush()
        sig = generate_signature(tf.name)
        assert len(sig) == 252
        delta = pack_delta(generate_delta(tf.name, sig))
        assert len(delta) == 213

        # ensure we generate the same content
        with tempfile.NamedTemporaryFile() as tf_out:
            patch(tf.name, tf_out.name, sig, delta)
            data = tf_out.read()
            tf.seek(0)
            assert len(data) == 2**16
            assert data == tf.read()
