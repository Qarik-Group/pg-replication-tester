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
	PgrtVersion = "v1.0.1"
	/* exit codes... */
	BecauseConnectionFailed     = 2
	BecauseMasterQueryFailed    = 3
	BecauseSlaveQueryFailed     = 4
	BecauseXlogConversionFailed = 5
	BecauseReplicationLag       = 6
)

var debugging = false

func debug(f string, args ...interface{}) {
	if debugging {
		fmt.Fprintf(os.Stderr, "DEBUG> %s\n", fmt.Sprintf(f, args))
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

func xlog(s string) int64 {
	l := strings.SplitN(s, "/", 2)
	if len(l) != 2 {
		fmt.Fprintf(os.Stderr, "xlog(%s) failed - not a valid xlog location?\n", s)
		os.Exit(BecauseXlogConversionFailed)
	}

	a, err := strconv.ParseInt(l[0], 16, 64)
	if err != nil {
		fmt.Fprintf(os.Stderr, "xlog(%s) failed - not a valid xlog location?\n", s)
		os.Exit(BecauseXlogConversionFailed)
	}

	b, err := strconv.ParseInt(l[1], 16, 64)
	if err != nil {
		fmt.Fprintf(os.Stderr, "xlog(%s) failed - not a valid xlog location?\n", s)
		os.Exit(BecauseXlogConversionFailed)
	}

	return a<<64 + b
}

type Master struct {
	name          string
	xlog_location string
}

func QueryMaster(host, port, user, pass, dbname string) (m Master) {
	m.name = host
	db := connect(host, port, user, pass, dbname)
	defer db.Close()
	var err error
	if m.xlog_location, err = query1(db, "SELECT pg_current_xlog_location()"); err != nil {
		fmt.Printf("Failed to query current xlog location: %s\n", err)
		os.Exit(BecauseMasterQueryFailed)
	}
	return
}

type Slave struct {
	name          string
	recv_location string
	rply_location string
	behind        int64
	delay         int64
}

func QuerySlave(host, port, user, pass, dbname string) (s Slave) {
	s.name = host
	db := connect(host, port, user, pass, dbname)
	defer db.Close()
	var err error
	if s.recv_location, err = query1(db, "SELECT pg_last_xlog_receive_location()"); err != nil {
		fmt.Printf("Failed to query last received xlog location: %s\n", err)
		os.Exit(BecauseSlaveQueryFailed)
	}

	if s.rply_location, err = query1(db, "SELECT pg_last_xlog_replay_location()"); err != nil {
		fmt.Printf("Failed to query last replayed xlog location: %s\n", err)
		os.Exit(BecauseSlaveQueryFailed)
	}
	return
}

func (s *Slave) Check(m Master) {
	debug("checking master xlog_location: `%s`, slave recv_location: `%s`, slave rply_location: `%s`",
		m.xlog_location, s.recv_location, s.rply_location)
	s.behind = xlog(s.recv_location) - xlog(m.xlog_location)
	s.delay = xlog(s.rply_location) - xlog(s.recv_location)
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
		AcceptLag int64    `goptions:"-l, --lag, description='Maximum acceptable lag behind the master xlog position (bytes)'"`
		Version   bool     `goptions:"-v, --version, description='Program version'"`
	}{
		Port:      "6432",
		AcceptLag: 8192,
	}
	goptions.ParseAndFail(&options)
	if options.Version {
		fmt.Printf("%s version `%s`\n", PgrtName, PgrtVersion)
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

	fmt.Printf("%s: %s\n", master.name, master.xlog_location)
	failed := false
	for _, slave := range slaves {
		slave.Check(master)

		emsg := ""
		if slave.behind > options.AcceptLag {
			failed = true
			emsg = "    !! too far behind write master"
		}

		fmt.Printf("%s: %s %-12s   to %s %-12s%s\n", slave.name,
			slave.recv_location, fmt.Sprintf("(%d)", -1*slave.behind),
			slave.rply_location, fmt.Sprintf("(%d)", -1*slave.delay),
			emsg)
	}
	if failed {
		fmt.Print("FAILED\n")
		os.Exit(BecauseReplicationLag)
	}
}
