package main

import (
	"database/sql"
	"fmt"
	"math"
	"os"
	"strconv"

	_ "github.com/lib/pq"
	"github.com/voxelbrain/goptions"
)

var debugging = false

func debug(f string, args ...interface{}) {
	if debugging {
		fmt.Fprintf(os.Stderr, "DEBUG> %s\n", fmt.Sprintf(f, args))
	}
}

func query1(db *sql.DB, q string) (string, error) {
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

func main() {
	options := struct {
		Master       string   `goptions:"-M, --master, description='Replication master host.  May only be specified once'"`
		Slaves       []string `goptions:"-S, --slave, description='Replication slave host(s).  May be specified more than once'"`
		FrontendPort string   `goptions:"-P, --pgpool-port, description='TCP port that PGPoolII listens on'"`
		BackendPort  string   `goptions:"-p, --postgres-port, description='TCP port that Postgres listens on'"`
		User         string   `goptions:"-u, --user, description='User to connect as'"`
		Password     string   `goptions:"-w, --password, description='Password to connect with'"`
		Database     string   `goptions;"-d, --database, description='Database to use for testing'"`
		Debug        bool     `goptions:"-D, --debug, description='Enable debugging output (to standard error)'"`
		AcceptLag    string   `goptions:"-l, --lag, description='Maximum acceptable lag behind the master xlog position (bytes)'"`
	}{
		FrontendPort: "5432",
		BackendPort:  "6432",
		AcceptLag:    "8192",
	}
	goptions.ParseAndFail(&options)
	if options.Database == "" {
		options.Database = options.User
	}
	debugging = options.Debug

	var master_xlog string
	var failed bool

	func() {
		host := options.Master
		conn := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
			host, options.BackendPort, options.User, options.Password, options.Database)
		debug("checking on WRITE MASTER %s:%s", host, options.BackendPort)
		debug("  (connecting to: %s)", conn)

		db, err := sql.Open("postgres", conn)
		if err != nil {
			failed = true
			fmt.Printf("Error connecting to write master %s:%s as user %s, database %s: %s\n",
				host, options.BackendPort, options.User, options.Database, err)
			return
		}
		defer db.Close()

		debug("connected.  checking replication status...")
		master_xlog, err = query1(db, "SELECT pg_current_xlog_location()")
		if err != nil {
			fmt.Printf("Error querying pg_current_xlog_location(): %s\n", err)
			return
		}
		fmt.Printf("%s: %s\n", host, master_xlog)
	}()

	for _, host := range options.Slaves {
		func() {
			conn := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
				host, options.BackendPort, options.User, options.Password, options.Database)
			debug("checking on READ SLAVE %s:%s", host, options.BackendPort)
			debug("  (connecting to: %s)", conn)

			db, err := sql.Open("postgres", conn)
			if err != nil {
				failed = true
				fmt.Printf("Error connecting to read slave %s:%s as user %s, database %s: %s\n",
					host, options.BackendPort, options.User, options.Database, err)
			}
			defer db.Close()

			debug("connected.  checking replication status...")
			recv, err := query1(db, "SELECT pg_last_xlog_receive_location()")
			if err != nil {
				failed = true
				fmt.Printf("Error querying pg_last_xlog_receive_location(): %s\n", err)
				return
			}

			replay, _ := query1(db, "SELECT pg_last_xlog_replay_location()")
			behind, _ := query1(db, "SELECT pg_xlog_location_diff('"+master_xlog+"', '"+recv+"')")
			lag, _ := query1(db, "SELECT pg_xlog_location_diff('"+recv+"', '"+replay+"')")

			fmt.Printf("%s: %s %-12s   to %s %-12s",
				host,
				recv, fmt.Sprintf("(%s)", behind),
				replay, fmt.Sprintf("(%s)", lag))

			n, _ := strconv.ParseFloat(behind, 64)
			e, _ := strconv.ParseFloat(options.AcceptLag, 64)

			if math.Abs(n) > math.Abs(e) {
				failed = true
				fmt.Printf("    !! too far behind write master\n")
				return
			}

			fmt.Printf("\n")
		}()
	}

	if failed {
		fmt.Print("FAILED\n")
		os.Exit(1)
	}
}
