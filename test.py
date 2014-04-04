import pwq

mk = pwq.MkWorkQueue().replicate(1).port(9123)
q = mk() #pwq.WorkQueue(mk)


for i in xrange(10):
    t = pwq.Task('bash -c "echo -n hello %s"' % i)
    q.submit(t)

while not q.empty():
    r = q.wait(5)
    if r:
        print r.id, r.result


