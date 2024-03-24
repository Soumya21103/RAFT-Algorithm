class A:
    def __init__(self):
        self.a = 1

class B:
    def __init__(self, an : A) -> None:
        self.an = an

x = A()
y = B(x)
x.a += 1
print(x.a,y.an.a)
y.an.a += 1
print(x.a,y.an.a)



