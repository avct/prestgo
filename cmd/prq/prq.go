// Command prq is a command line interface for running presto queries
package main

import (
	"database/sql"
	"fmt"
	"os"
	"text/tabwriter"

	_ "github.com/avct/prestgo"
)

func main() {
	if len(os.Args) < 2 {
		fatal("missing required data source argument")
	}

	if len(os.Args) < 3 {
		fatal("missing required query argument")
	}

	db, err := sql.Open("prestgo", os.Args[1])
	if err != nil {
		fatal(fmt.Sprintf("failed to connect to presto: %v", err))
	}
	rows, err := db.Query(os.Args[2])
	if err != nil {
		fatal(fmt.Sprintf("failed query presto: %v", err))
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		fatal(fmt.Sprintf("failed to read columns: %v", err))
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 8, 0, '\t', 0)
	defer w.Flush()

	for i := range cols {
		if i > 0 {
			fmt.Fprint(w, "\t")
		}
		fmt.Fprintf(w, "%v", cols[i])
	}
	fmt.Fprint(w, "\n")

	data := make([]interface{}, len(cols))
	args := make([]interface{}, len(data))
	for i := range data {
		args[i] = &data[i]
	}
	for rows.Next() {
		if err := rows.Scan(args...); err != nil {
			fatal(err.Error())
		}
		for i := range data {
			if i > 0 {
				fmt.Fprint(w, "\t")
			}
			fmt.Fprintf(w, "%v", data[i])
		}
		fmt.Fprint(w, "\n")
	}
	if err := rows.Err(); err != nil {
		fatal(err.Error())
	}
}

func fatal(msg string) {
	fmt.Fprintln(os.Stderr, msg)
	os.Exit(1)
}
