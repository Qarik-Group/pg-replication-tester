package main

import (
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"strings"

	_ "github.com/lib/pq"
	"github.com/voxelbrain/goptions"
)

const (
	PgrtName    = "PG Replication Tester"
	PgrtVersion = "v1.1.0"
	/* exit codes... */
	BecauseConnectionFailed    = 2
	BecauseMasterQueryFailed   = 3
	BecauseSlaveQueryFailed    = 4
	BecauseWalConversionFailed = 5
	BecauseReplicationLag      = 6
)

var debugging = false

func debug(f string, a ...interface{}) {
	if debugging {
		fmt.Printf("DEBUG> "+f+"\n", a...)
	}
}

func query1(db *sql.DB, q string) (string, error) {
	debug("query: `%s`", q)
	r, err := db.Query(q)
	if err != nil {
		return "", err
	}

	var v string
	r.Next()
	if err = r.Scan(&v); err != nil {
		return "", err
	}

	debug("query result: `%s`", v)
	return v, nil
}

func connect(host, port, user, password, dbname string) *sql.DB {
	conn := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		host, port, user, password, dbname)
	debug("connecting to `%s`", conn)

	db, err := sql.Open("postgres", conn)
	if err != nil {
		fmt.Printf("Error connecting to %s:%s as user %s, database %s: %s\n",
			host, port, user, dbname, err)
		os.Exit(BecauseConnectionFailed)
		return nil
	}

	return db
}

// https://pgpedia.info/p/pg_lsn.html
func parsePgLsn(s string) uint64 {
	const supportInfo = "parser supports values between 0/0 and FFFFFFFF/FFFFFFFF"
	l := strings.SplitN(s, "/", 3)
	if len(l) != 2 {
		fmt.Fprintf(os.Stderr, "parsing pg_lsn=`%s` failed (not two parts?), %s\n", s, supportInfo)
		os.Exit(BecauseWalConversionFailed)
	}

	a, err := strconv.ParseUint(l[0], 16, 64)
	if err != nil {
		fmt.Fprintf(os.Stderr, "parsing pg_lsn=`%s` failed (first part), %s\n", s, supportInfo)
		os.Exit(BecauseWalConversionFailed)
	}

	b, err := strconv.ParseUint(l[1], 16, 64)
	if err != nil {
		fmt.Fprintf(os.Stderr, "parsing pg_lsn=`%s` failed (second part), %s\n", s, supportInfo)
		os.Exit(BecauseWalConversionFailed)
	}

	return a<<32 + b
}

type Master struct {
	name          string
	currentWalLsn string // lsn - Log Sequence Number
}

func QueryMaster(host, port, user, pass, dbname string) (m Master) {
	m.name = host
	db := connect(host, port, user, pass, dbname)
	defer db.Close()
	var err error
	// https://www.postgresql.org/docs/current/functions-admin.html
	// pg_current_xlog_location() -> pg_current_wal_lsn()
	if m.currentWalLsn, err = query1(db, "SELECT pg_current_wal_lsn()"); err != nil {
		fmt.Printf("Failed to query current wal location: %s\n", err)
		os.Exit(BecauseMasterQueryFailed)
	}
	return
}

type Slave struct {
	name              string
	lastWalReceiveLsn string
	lastWalReplayLsn  string
}

func QuerySlave(host, port, user, pass, dbname string) (s Slave) {
	s.name = host
	db := connect(host, port, user, pass, dbname)
	defer db.Close()
	var err error
	// https://www.postgresql.org/docs/current/functions-admin.html
	// pg_last_xlog_receive_location() -> pg_last_wal_receive_lsn()
	// pg_last_xlog_replay_location() -> pg_last_wal_replay_lsn()
	// pg_xlog_location_diff() ->  pg_wal_lsn_diff()
	if s.lastWalReceiveLsn, err = query1(db, "SELECT pg_last_wal_receive_lsn()"); err != nil {
		fmt.Printf("Failed to query last received wal location: %s\n", err)
		os.Exit(BecauseSlaveQueryFailed)
	}

	if s.lastWalReplayLsn, err = query1(db, "SELECT pg_last_wal_replay_lsn()"); err != nil {
		fmt.Printf("Failed to query last replayed wal location: %s\n", err)
		os.Exit(BecauseSlaveQueryFailed)
	}
	return
}

func (s *Slave) CalculateLag(m Master) (behind uint64, delay uint64) {
	debug("checking: master.currentWalLsn=`%s`, slave.lastWalReceiveLsn=`%s`, slave.lastWalReplayLsn=`%s`", m.currentWalLsn, s.lastWalReceiveLsn, s.lastWalReplayLsn)
	masterCurrentWalLsn := parsePgLsn(m.currentWalLsn)
	slaveLastWalReceive := parsePgLsn(s.lastWalReceiveLsn)
	slaveLastWalReplay := parsePgLsn(s.lastWalReplayLsn)
	behind = 0
	if masterCurrentWalLsn > slaveLastWalReceive {
		behind = masterCurrentWalLsn - slaveLastWalReceive
	}
	delay = 0
	if slaveLastWalReceive > slaveLastWalReplay {
		delay = slaveLastWalReceive - slaveLastWalReplay
	}
	debug("checking: master.currentWalLsn=%d, slave.lastWalReceive=%d, slave.lastWalReplay=%d, slave.behind=%d, slave.delay=%d", masterCurrentWalLsn, slaveLastWalReceive, slaveLastWalReplay, behind, delay)
	return behind, delay
}

func main() {
	options := struct {
		Master    string   `goptions:"-M, --master, description='Replication master host.  May only be specified once'"`
		Slaves    []string `goptions:"-S, --slave, description='Replication slave host(s).  May be specified more than once'"`
		Port      string   `goptions:"-p, --port, description='TCP port that Postgres listens on'"`
		Database  string   `goptions:"-d, --database, description='Database to use for testing'"`
		User      string   `goptions:"-u, --user, description='User to connect as'"`
		Password  string   `goptions:"-w, --password, description='Password to connect with'"`
		Debug     bool     `goptions:"-D, --debug, description='Enable debugging output (to standard error)'"`
		AcceptLag int64    `goptions:"-l, --lag, description='Maximum acceptable lag behind the master wal position (bytes)'"`
		Version   bool     `goptions:"-v, --version, description='Output version information, then exit'"`
		Help      bool     `goptions:"-h, --help, description='Show this help, then exit'"`
	}{
		Port:      "6432",
		AcceptLag: 8192,
	}
	goptions.ParseAndFail(&options)
	if options.Help {
		fmt.Printf("%s version `%s`\n", PgrtName, PgrtVersion)
		goptions.PrintHelp()
		os.Exit(0)
	}
	if options.Version {
		fmt.Printf("%s version `%s`\n", PgrtName, PgrtVersion)
		os.Exit(0)
	}
	if options.Database == "" {
		options.Database = options.User
	}
	debugging = options.Debug

	var slaves = make([]Slave, len(options.Slaves))
	for i, host := range options.Slaves {
		slaves[i] = QuerySlave(host, options.Port,
			options.User, options.Password, options.Database)
	}

	master := QueryMaster(options.Master, options.Port,
		options.User, options.Password, options.Database)

	fmt.Printf("%s: %s\n", master.name, master.currentWalLsn)
	failed := false
	for _, slave := range slaves {
		behind, delay := slave.CalculateLag(master)

		emsg := ""
		if behind > uint64(options.AcceptLag) {
			failed = true
			emsg = "    !! too far behind write master"
		}

		fmt.Printf("%s: %s (behind %d) to %s (delay %d) %s\n", slave.name,
			slave.lastWalReceiveLsn, behind, slave.lastWalReplayLsn, delay, emsg)
	}
	if failed {
		fmt.Print("FAILED\n")
		os.Exit(BecauseReplicationLag)
	}
}
