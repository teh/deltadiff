import sys
import collections
from math import log

data = sys.stdin.read()
c = collections.Counter(data)
n = float(len(data))
print - sum(x/n * log(x/n, 256) for x in c.values())
