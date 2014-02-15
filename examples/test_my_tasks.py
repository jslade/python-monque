from monque import Monque
from my_tasks import *

q = Monque()

plus = Add().post([1,2])
minus = Subtract().post([1,2])

print "result of plus:", plus.wait()
print "result of minus:", minus.wait()
