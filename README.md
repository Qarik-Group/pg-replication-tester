Postgres Replication Tester
===========================

This repository houses `pgrt`, a small utility that will connect
to all the nodes in a PostgreSQL streaming replication cluster
and verify the health and well-being of each node.

Example
-------

A healthy cluster, with some leeway on the replication lag (`-l`)

```
$ pgrt -M 10.244.232.2 -S 10.244.232.3 -S 10.244.232.4 -l 65536

[2023-03-24T10:37:17Z] Master 10.244.232.2 current wal LSN 731586560 (0/2B9B2000)
[2023-03-24T10:37:17Z] Slave 10.244.232.3(true) receives wals with lag 0, replays received wals with lag 72 - max accepted lag 65536 - keeps up with master
[2023-03-24T10:37:18Z] Slave 10.244.232.4(true) receives wals with lag 592, replays received wals with lag 0 - max accepted lag 65536 - keeps up with master
```

The same cluster, reporting as unhealthy because we only tolerate
800 bytes of replication lag (admittedly, fairly unrealistic):

```
$ pgrt -M 10.244.232.2 -S 10.244.232.3 -S 10.244.232.4 -l 800

[2023-03-24T10:43:51Z] Master 10.244.232.2 current wal LSN 798990336 (0/2F9FA000)
[2023-03-24T10:43:50Z] Slave 10.244.232.3(true) receives wals with lag 0, replays received wals with lag 56 - max accepted lag 800 - keeps up with master
[2023-03-24T10:43:51Z] Slave 10.244.232.4(true) receives wals with lag 840, replays received wals with lag 0 - max accepted lag 800 - is too far behind write master
[2023-03-24T10:43:51Z] FAILED (lagging slaves 1)

```

Debugging:
```
$ pgrt -M 10.244.232.2 -S 10.244.232.3 -l 800 --debug

[2023-03-24T10:46:11Z] DEBUG> connecting to `host=10.244.232.2 port=6432 ...`
[2023-03-24T10:46:11Z] DEBUG> query: `SELECT pg_is_in_recovery()`
[2023-03-24T10:46:11Z] DEBUG> query result: `false`
[2023-03-24T10:46:11Z] DEBUG> query: `SELECT pg_current_wal_lsn()`
[2023-03-24T10:46:11Z] DEBUG> query result: `0/3137E000`
[2023-03-24T10:46:11Z] DEBUG> connecting to `host=10.244.232.3 port=6432 ...`
[2023-03-24T10:46:11Z] DEBUG> query: `SELECT pg_is_in_recovery()`
[2023-03-24T10:46:11Z] DEBUG> query result: `true`
[2023-03-24T10:46:11Z] DEBUG> query: `SELECT pg_last_wal_receive_lsn()`
[2023-03-24T10:46:11Z] DEBUG> query result: `0/31000000`
[2023-03-24T10:46:11Z] DEBUG> query: `SELECT pg_last_wal_replay_lsn()`
[2023-03-24T10:46:11Z] DEBUG> query result: `0/3112B198`
[2023-03-24T10:46:11Z] Master 10.244.232.2 current wal LSN 825745408 (0/3137E000)
[2023-03-24T10:46:11Z] DEBUG> calculate lag between master 10.244.232.2 and slave 10.244.232.3:
  master.currentWalLsn    = 825745408 (0/3137E000)
  slave.lastWalReceiveLsn = 822083584 (0/31000000)
  slave.lastWalReplayLsn  = 823308696 (0/3112B198)
  slave.receiveLag        = 3661824
  slave.replayLag         = 0
[2023-03-24T10:46:11Z] Slave 10.244.232.3(true) receives wals with lag 3661824, replays received wals with lag 0 - max accepted lag 800 - is too far behind write master
[2023-03-24T10:46:11Z] FAILED (lagging slaves 1)
```


Exit Codes
----------

`pgrt` exits 0 if it can contact all nodes, each node is playing
the part specified (i.e. write master is a writing master, and read
slaves are actually reading slaves), and the replication lag (first
parenthetical figure) is below the acceptable lag (per `-l`)

It exists non-zero on failure, with the following meanings:

- **1** - Option processing or other non-runtime error. Check your flags.
- **2** - Connectivity to at least one node failed.
- **3** - A query to the write master failed
- **4** - A query to one of the read slaves failed
- **5** - pg_lsn conversion failed (if this happens, something is terribly broken...)
- **6** - One or more of the read slaves was lagging too far behind the master (based on `-l`)
- **7** - the master is in recovery mode or some slaves aren't in recovery mode

Options
-------

```
-M, --master   Replication master host.  May only be specified once
-S, --slave    Replication slave host(s).  May be specified more than once
-p, --port     TCP port that Postgres listens on (default: 6432)
-u, --user     User to connect as
-w, --password Password to connect with
-d, --database Database to use for testing, default: same as the User
-l, --lag      Maximum acceptable slave receive and replay lag (default: 8192)
-D, --debug    Enable debugging output (to standard error)
-v, --version  Output version information, then exit
-h, --help     Show this help, then exit
```

Building
--------

```bash
GOOS=linux GOARCH=amd64 go build -o pgrt
```
