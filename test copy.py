import time
s = time.clock_gettime_ns(1)
f = open("random copy.txt","r+")
TILL= 5
l = 0
c = f.readline()

while not c != "":
    l += 1
    c = f.readline()

for i in range(l):
    f.seek(0)
    for j in range(i):
        f.readline()

print(time.clock_gettime_ns(1)- s)