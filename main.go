package main

import (
	"database/sql"
	"fmt"
	_ "github.com/lib/pq"
	"github.com/voxelbrain/goptions"
	"os"
	"strconv"
	"strings"
)

const (
	PgrtName    = "PG Replication Tester"
	PgrtVersion = "v1.1.1"
	/* exit codes... */
	BecauseConnectionFailed    = 2
	BecauseMasterQueryFailed   = 3
	BecauseSlaveQueryFailed    = 4
	BecauseWalConversionFailed = 5
	BecauseReplicationLag      = 6
	BecauseWrongInRecovery     = 7
)

func queryStr(db *sql.DB, q string) (string, error) {
	log.debug("query: `%s`", q)
	row := db.QueryRow(q)
	var v string
	if err := row.Scan(&v); err != nil {
		return "", err
	}
	log.debug("query result: `%v`", v)
	return v, nil
}

func connect(host, port, user, password, dbname string) *sql.DB {
	conn := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		host, port, user, password, dbname)
	log.debug("connecting to `%s`", conn)

	db, err := sql.Open("postgres", conn)
	if err != nil {
		log.error("Error connecting to %s:%s as user %s, database %s: %s", host, port, user, dbname, err)
		os.Exit(BecauseConnectionFailed)
		return nil
	}

	return db
}

// https://pgpedia.info/p/pg_lsn.html
func parsePgLsn(s string) uint64 {
	const supportInfo = "parser supports values between 0/0 and FFFFFFFF/FFFFFFFF"
	if s == "" {
		s = "0/0"
	}
	l := strings.SplitN(s, "/", 3)
	if len(l) != 2 {
		log.error("parsing pg_lsn=`%s` failed (not two parts?), %s", s, supportInfo)
		os.Exit(BecauseWalConversionFailed)
	}

	a, err := strconv.ParseUint(l[0], 16, 64)
	if err != nil {
		log.error("parsing pg_lsn=`%s` failed (first part), %s", s, supportInfo)
		os.Exit(BecauseWalConversionFailed)
	}

	b, err := strconv.ParseUint(l[1], 16, 64)
	if err != nil {
		log.error("parsing pg_lsn=`%s` failed (second part), %s", s, supportInfo)
		os.Exit(BecauseWalConversionFailed)
	}

	return a<<32 + b
}

func queryIsInRecovery(db *sql.DB) bool {
	var isInRecoveryStr string
	var err error
	if isInRecoveryStr, err = queryStr(db, "SELECT pg_is_in_recovery()"); err != nil {
		log.error("Failed to query recovery mode: %s", err)
		os.Exit(BecauseMasterQueryFailed)
	}
	return isInRecoveryStr == "t" || isInRecoveryStr == "true"
}

type Master struct {
	name             string
	isInRecovery     bool
	currentWalLsn    string // lsn - Log Sequence Number
	currentWalLsnInt uint64
}

func QueryMaster(host, port, user, pass, dbname string) (m Master) {
	m.name = host
	db := connect(host, port, user, pass, dbname)
	defer db.Close()
	m.isInRecovery = queryIsInRecovery(db)
	var err error
	// https://www.postgresql.org/docs/current/functions-admin.html
	// pg_current_xlog_location() -> pg_current_wal_lsn()
	if m.currentWalLsn, err = queryStr(db, "SELECT COALESCE(pg_current_wal_lsn(),'0/0)"); err != nil {
		log.error("Failed to query current wal location, host: %s, port: %s, error: %v", host, port, err)
		// TODO: err when is a slave: Failed to query current wal location: pq: recovery is in progress
		os.Exit(BecauseMasterQueryFailed)
	}
	m.currentWalLsnInt = parsePgLsn(m.currentWalLsn)
	return
}

type Slave struct {
	name                 string
	isInRecovery         bool
	lastWalReceiveLsn    string
	lastWalReceiveLsnInt uint64
	lastWalReplayLsn     string
	lastWalReplayLsnInt  uint64
}

func QuerySlave(host, port, user, pass, dbname string) (s Slave) {
	s.name = host
	db := connect(host, port, user, pass, dbname)
	defer db.Close()
	s.isInRecovery = queryIsInRecovery(db)
	var err error
	// https://www.postgresql.org/docs/current/functions-admin.html
	// pg_last_xlog_receive_location() -> pg_last_wal_receive_lsn()
	// pg_last_xlog_replay_location() -> pg_last_wal_replay_lsn()
	// pg_xlog_location_diff() ->  pg_wal_lsn_diff()
	if s.lastWalReceiveLsn, err = queryStr(db, "SELECT COALESCE(pg_last_wal_receive_lsn(),'0/0)"); err != nil {
		log.error("Failed to query last received wal location, host: %s, port: %s, error: %v", host, port, err)
		os.Exit(BecauseSlaveQueryFailed)
	}
	s.lastWalReceiveLsnInt = parsePgLsn(s.lastWalReceiveLsn)

	if s.lastWalReplayLsn, err = queryStr(db, "SELECT COALESCE(pg_last_wal_replay_lsn(),'0/0)'"); err != nil {
		log.error("Failed to query last replayed wal location, host: %s, port: %s, error: %v", host, port, err)
		os.Exit(BecauseSlaveQueryFailed)
	}
	s.lastWalReplayLsnInt = parsePgLsn(s.lastWalReplayLsn)
	return
}

func (s *Slave) CalculateLag(m Master) (receiveLag uint64, replayLag uint64) {
	receiveLag = 0
	if m.currentWalLsnInt > s.lastWalReceiveLsnInt {
		receiveLag = m.currentWalLsnInt - s.lastWalReceiveLsnInt
	}
	replayLag = 0
	if s.lastWalReceiveLsnInt > s.lastWalReplayLsnInt {
		replayLag = s.lastWalReceiveLsnInt - s.lastWalReplayLsnInt
	}
	log.debug("calculate lag between master %s and slave %s:\n"+
		"  master.currentWalLsn    = %d (%s)\n"+
		"  slave.lastWalReceiveLsn = %d (%s)\n"+
		"  slave.lastWalReplayLsn  = %d (%s)\n"+
		"  slave.receiveLag        = %d\n"+
		"  slave.replayLag         = %d",
		m.name, s.name,
		m.currentWalLsnInt, m.currentWalLsn,
		s.lastWalReceiveLsnInt, s.lastWalReceiveLsn,
		s.lastWalReplayLsnInt, s.lastWalReplayLsn,
		receiveLag, replayLag)
	return receiveLag, replayLag
}

func main() {
	options := struct {
		Master    string   `goptions:"-M, --master, description='Replication master host.  May only be specified once'"`
		Slaves    []string `goptions:"-S, --slave, description='Replication slave host(s).  May be specified more than once'"`
		Port      string   `goptions:"-p, --port, description='TCP port that Postgres listens on'"`
		User      string   `goptions:"-u, --user, description='User to connect as'"`
		Password  string   `goptions:"-w, --password, description='Password to connect with'"`
		Database  string   `goptions:"-d, --database, description='Database to use for testing, default: same as the User'"`
		AcceptLag int64    `goptions:"-l, --lag, description='Maximum acceptable slave receive and replay lag (bytes)'"`
		Debug     bool     `goptions:"-D, --debug, description='Enable debugging output (to standard error)'"`
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
	log.debugging = options.Debug

	master := QueryMaster(options.Master, options.Port, options.User, options.Password, options.Database)
	if master.isInRecovery {
		log.error("FAILED (master %s is in recovery: %t)", master.name, master.isInRecovery)
		os.Exit(BecauseWrongInRecovery)
	}
	var slaves = make([]Slave, len(options.Slaves))
	for i, host := range options.Slaves {
		slaves[i] = QuerySlave(host, options.Port, options.User, options.Password, options.Database)
	}

	log.info("Master %s current wal LSN %d (%s)", master.name, master.currentWalLsnInt, master.currentWalLsn)
	laggingSlavesCount := 0
	notInRecoverySlavesCount := 0
	for _, slave := range slaves {
		receiveLag, replayLag := slave.CalculateLag(master)
		if !slave.isInRecovery {
			notInRecoverySlavesCount++
		}

		emsg := "keeps up with master"
		if receiveLag > uint64(options.AcceptLag) || replayLag > uint64(options.AcceptLag) {
			laggingSlavesCount++
			emsg = "is too far behind write master"
		}

		log.info("Slave %s(%t) receives wals with lag %d, replays received wals with lag %d - max accepted lag %d - %s",
			slave.name, slave.isInRecovery, receiveLag, replayLag, options.AcceptLag, emsg)
	}
	if notInRecoverySlavesCount > 0 {
		log.error("FAILED (some slaves aren't in recovery: %d)", master.isInRecovery, notInRecoverySlavesCount)
		os.Exit(BecauseWrongInRecovery)
	}
	if laggingSlavesCount > 0 {
		log.error("FAILED (lagging slaves %d)", laggingSlavesCount)
		os.Exit(BecauseReplicationLag)
	}
}
