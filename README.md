# prestgo

A pure [Go](http://golang.org/) database driver for the [Presto](http://prestodb.io/) query engine.

## Installation

Simply run

	go get github.com/avct/prestgo/...

This will install the Presto database driver and the `prq` tool for running queries fro the command line.

Documentation is at http://godoc.org/github.com/avct/prestgo

## Usage

Prestgo conforms to the Go standard library [Driver interface](http://golang.org/pkg/database/sql/driver/#Driver). This means it works transparently with the [`database/sql`](http://golang.org/pkg/database/sql/) package. Simply import the `github.com/avct/prestgo` package to auto-register the driver:

```
import "github.com/avct/prestgo"
```

If you don't intend to use any prestgo-specific functions directly, you can import using the blank identifier which will still register the driver:

```
import _ "github.com/avct/prestgo"
```

The driver name is `prestgo` and it supports the standard Presto data source name format `presto://hostname:port/catalog/schema`. All parts of the data source name are optional, defaulting to port 8080 on localhost with `hive` catalog and `default` schema.

Here's how to get a list of tables from a Presto server:

```Go
package main

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/avct/prestgo"
)

func main() {
	db, err := sql.Open("prestgo", "presto://example:8080/hive/default")
	if err != nil {
		log.Fatalf("failed to connect to presto: %v", err)
	}

	rows, err := db.Query("SHOW TABLES")
	if err != nil {
		log.Fatalf("failed to run query: %v", err)
	}

	defer rows.Close()

	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			log.Fatal(err.Error())
		}

		fmt.Printf("%s\n", name)
	}
	if err := rows.Err(); err != nil {
		log.Fatal(err.Error())
	}
}
```


## Features

* SELECT, SHOW, DESCRIBE
* Pagination of results
* `varchar`, `bigint`, `boolean`, `double` and `timestamp` datatypes
* Custom HTTP clients

## Future 

(aka: Things you could help with)

* Parameterised queries
* INSERT queries
* DDL (ALTER/CREATE/DROP TABLE)
* Cancelling of queries
* User authentication
* `json`, `date`, `time`, `interval`, `array`, `row` and `map` datatypes


## Authors

Originally written by [Ian Davis](http://iandavis.com).

## Contributors

Your name here...

## Contributing

* Do submit your changes as a pull request
* Do your best to adhere to the existing coding conventions and idioms.
* Do supply unit tests if possible.
* Do run `go fmt` on the code before committing 
* Do feel free to add yourself to the Contributors list in
  the [`README.md`](README.md).  Alphabetical order applies.
* Don't touch the Authors section. An existing author will add you if 
  your contributions are significant enough.

## License

This software is released under the MIT License, please see the accompanying [`LICENSE`](LICENSE) file.

