import time
s = time.clock_gettime_ns(1)
f = open("random.txt","r+")
TILL= 5
l = 0
c = f.readline()
seek_loc = []
while  c != '':
    l += 1
    c = f.readline()
    if(l%25 == 0):
        seek_loc.append(f.tell())

for i in seek_loc[::-1]:
    f.seek(i)
    for j in range(25):
        f.readline()

print(time.clock_gettime_ns(1)- s)





