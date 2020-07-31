package main

import (
	"database/sql"
	"fmt"
	"github.com/ClickHouse/clickhouse-go"
	"log"
	"net/url"
	"os"
	"time"
)

func perf_insert(connect *sql.DB) {
	_, err := connect.Exec(`DROP TABLE IF EXISTS perf_go`)
	if err != nil {
		log.Fatal(err)
	}

	_, err = connect.Exec(`
		CREATE TABLE IF NOT EXISTS perf_go (
          id  UInt32,
          name  String,
          dt   DateTime 
        ) Engine=MergeTree PARTITION BY name ORDER BY dt	
	`)

	if err != nil {
		log.Fatal(err)
	}

	NAMES := [5]string{"one", "two", "three", "four", "five"}
	start := time.Now()

	for l := 0; l < 1000; l++ {
		tx, _ := connect.Begin()
		stmt, err := tx.Prepare("INSERT INTO perf_go (id, name, dt) VALUES (?, ?, ?)")

		if err != nil {
			log.Fatal(err)
		}

		now := time.Now()

		for i := 0; i < 10000; i++ {
			if _, err := stmt.Exec(
				l,

				NAMES[i%len(NAMES)],
				now.Add(time.Duration(i)*time.Second),
			); err != nil {
				log.Fatal(err)
			}
		}

		if err := tx.Commit(); err != nil {
			log.Fatal(err)
		}

		stmt.Close()
	}
	elapsed := time.Now().Sub(start)
	fmt.Printf("elapsed %v msec\n", elapsed.Milliseconds())
}

func perf_select(connect *sql.DB) {
	start := time.Now()

	rows, err := connect.Query("SELECT id,name,dt FROM perf")
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()
	var sum uint64 = 0

	for rows.Next() {
		var (
			name string
			id   uint32
			dt   time.Time
		)
		if err := rows.Scan(&id, &name, &dt); err != nil {
			log.Fatal(err)
		}
		sum += uint64(id)
	}

	if err := rows.Err(); err != nil {
		log.Fatal(err)
	}

	elapsed := time.Now().Sub(start)
	fmt.Printf("elapsed %v msec\n", elapsed.Milliseconds())
	fmt.Printf(" sum=%d", sum)
}

func main() {

	db_url := os.Getenv("DATABASE_URL")

	if db_url != "" {
		u, err := url.Parse(db_url)
		if err != nil {
			log.Fatal(err)
		}
		q := u.Query()
		if q.Get("compression") != "" {
			q.Del("compression")
			q.Set("compress", "true")
		}

		if v, ok := u.User.Password(); ok {
			q.Set("password", v)
		}

		v := u.User.Username()
		q.Set("username", v)

		u.RawQuery = q.Encode()

		db_url = u.String()
	} else {
		db_url = "tcp://default@localhost/default?compress=true"
	}

	fmt.Printf("url = %v\n", db_url)

	connect, err := sql.Open("clickhouse", db_url)
	if err != nil {
		log.Fatal(err)
	}
	if err := connect.Ping(); err != nil {
		if exception, ok := err.(*clickhouse.Exception); ok {
			fmt.Printf("[%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace)
		} else {
			fmt.Println(err)
		}
		return
	}

	//connection established
	meth := os.Args[1]
	if meth == "insert" {
		perf_insert(connect)
	} else if meth == "select" {
		perf_select(connect)
	} else {
		fmt.Printf("method not provided")
	}

}
