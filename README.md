Postgres Replication Tester
===========================

This repository houses `pgrt`, a small utility that will connect
to all of the nodes in a PostgreSQL streaming replication cluster
and verify the health and well-being of each node.

Example
-------

A healthy cluster, with some leeway on the replication lag (`-l)

```
$ pgrt -M 10.244.232.2 -S 10.244.232.3 -S 10.244.232.4 -l 31768
10.244.232.2: 0/B30BA98
10.244.232.3: 0/B30BA98 (0)            to 0/B30BC28 (-400)
10.244.232.4: 0/B30BDC0 (-808)         to 0/B30BDC0 (0)
```

The same cluster, reporting as unhealthy because we only tolerate
800 bytes of replication lag (admittedly, fairly unrealistic):

```
$ pgrt -M 10.244.232.2 -S 10.244.232.3 -S 10.244.232.4 -l 800
10.244.232.2: 0/B17F098
10.244.232.3: 0/B17F230 (-408)         to 0/B17F230 (0)
10.244.232.4: 0/B17F558 (-1216)        to 0/B17F6E8 (-400)          !! too far behind write master
FAILED
```
