// Command prestoschema shows information about a Presto schema
package main

import (
	"database/sql"
	"fmt"
	"os"

	_ "github.com/avct/prestgo"
)

func main() {
	if len(os.Args) < 2 {
		fatal("missing required data source argument")
	}

	db, err := sql.Open("prestgo", os.Args[1])
	if err != nil {
		fatal(fmt.Sprintf("failed to connect to presto: %v", err))
	}
	rows, err := db.Query("SHOW TABLES")
	if err != nil {
		fatal(fmt.Sprintf("failed to connect to presto: %v", err))
	}

	defer rows.Close()
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			fatal(err.Error())
		}
		fmt.Printf("%s\n", name)
	}
	if err := rows.Err(); err != nil {
		fatal(err.Error())
	}
}

func fatal(msg string) {
	fmt.Fprintln(os.Stderr, msg)
	os.Exit(1)
}
