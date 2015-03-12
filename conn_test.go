package prestgo

import (
	"database/sql/driver"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
)

func TestConfigParseDataSource(t *testing.T) {
	testCases := []struct {
		ds       string
		expected config
		error    bool
	}{

		{
			ds:       "",
			expected: config{"addr": ":8080", "catalog": "hive", "schema": "default"},
			error:    false,
		},

		{
			ds:       "presto://example:9000/",
			expected: config{"addr": "example:9000", "catalog": "hive", "schema": "default"},
			error:    false,
		},

		{
			ds:       "presto://example/",
			expected: config{"addr": "example:8080", "catalog": "hive", "schema": "default"},
			error:    false,
		},

		{
			ds:       "presto://example/tree",
			expected: config{"addr": "example:8080", "catalog": "tree", "schema": "default"},
			error:    false,
		},

		{
			ds:       "presto://example/tree/",
			expected: config{"addr": "example:8080", "catalog": "tree", "schema": "default"},
			error:    false,
		},

		{
			ds:       "presto://example/tree/birch",
			expected: config{"addr": "example:8080", "catalog": "tree", "schema": "birch"},
			error:    false,
		},
	}

	for _, tc := range testCases {
		conf := make(config)
		err := conf.parseDataSource(tc.ds)

		gotError := err != nil
		if gotError != tc.error {
			t.Errorf("got error=%v, wanted error=%v", gotError, tc.error)
			continue
		}

		if !reflect.DeepEqual(conf, tc.expected) {
			t.Errorf("%s: got %#v, wanted %#v", tc.ds, conf, tc.expected)
		}

	}
}

var oneRowColResponse = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/v1/query/abcd/1":
		fmt.Fprintln(w, fmt.Sprintf(`{
		  "id": "abcd",
		  "infoUri": "http://%[1]s/v1/query/abcd",
		  "partialCancelUri": "http://%[1]s/v1/query/abcd.0",
		  "columns": [
		    {
		      "name": "col0", "type": "varchar", "typeSignature": { "rawType": "varchar", "typeArguments": [], "literalArguments": [] }
		    }
		  ],
		  "data": [
		    [ "c0r0" ]
		  ]
		}`, r.Host))
	default:
		http.NotFound(w, r)
	}
})

var failingQueryResult = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/v1/query/abcd/1":
		fmt.Fprintln(w, fmt.Sprintf(`{
		  "id": "abcd",
		  "infoUri": "http://%[1]s/v1/query/abcd",
		  "partialCancelUri": "http://%[1]s/v1/query/abcd.0",
		  "stats":{"state":"FAILED"}
		}`, r.Host))
	default:
		http.NotFound(w, r)
	}
})

var supportedDatatypesResponse = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/v1/query/abcd/1":
		fmt.Fprintln(w, fmt.Sprintf(`{
		  "id": "abcd",
		  "infoUri": "http://%[1]s/v1/query/abcd",
		  "partialCancelUri": "http://%[1]s/v1/query/abcd.0",
		  "columns": [
		    { "name": "col0", "type": "varchar", "typeSignature": { "rawType": "varchar", "typeArguments": [], "literalArguments": [] } },
		    { "name": "col1", "type": "bigint", "typeSignature": { "rawType": "varchar", "typeArguments": [], "literalArguments": [] } },
		    { "name": "col2", "type": "double", "typeSignature": { "rawType": "varchar", "typeArguments": [], "literalArguments": [] } },
		    { "name": "col3", "type": "boolean", "typeSignature": { "rawType": "varchar", "typeArguments": [], "literalArguments": [] } }
		  ],
		  "data": [
		    [ "c0r0", 12345, 12.45, true ]
		  ]
		}`, r.Host))
	default:
		http.NotFound(w, r)
	}
})

var multiRowResponse = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/v1/query/abcd/1":
		fmt.Fprintln(w, fmt.Sprintf(`{
		  "id": "abcd",
		  "infoUri": "http://%[1]s/v1/query/abcd",
		  "partialCancelUri": "http://%[1]s/v1/query/abcd.0",
		  "columns": [
		    {
		      "name": "col0", "type": "varchar", "typeSignature": { "rawType": "varchar", "typeArguments": [], "literalArguments": [] }
		    }
		  ],
		  "data": [
		    [ "c0r0" ],
		    [ "c0r1" ],
		    [ "c0r2" ],
		    [ "c0r3" ],
		    [ "c0r4" ],
		    [ "c0r5" ]
		  ]
		}`, r.Host))
	default:
		http.NotFound(w, r)
	}
})

var multiPageResponse = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/v1/query/abcd/1":
		fmt.Fprintln(w, fmt.Sprintf(`{
		  "id": "abcd",
		  "infoUri": "http://%[1]s/v1/query/abcd",
		  "nextUri": "http://%[1]s/v1/query/abcd/2",
		  "partialCancelUri": "http://%[1]s/v1/query/abcd.0",
		  "columns": [
		    {
		      "name": "col0", "type": "varchar", "typeSignature": { "rawType": "varchar", "typeArguments": [], "literalArguments": [] }
		    }
		  ],
		  "data": [
		    [ "c0r0" ],
		    [ "c0r1" ],
		    [ "c0r2" ]
		  ]
		}`, r.Host))
	case "/v1/query/abcd/2":
		fmt.Fprintln(w, fmt.Sprintf(`{
		  "id": "abcd",
		  "infoUri": "http://%[1]s/v1/query/abcd",
		  "partialCancelUri": "http://%[1]s/v1/query/abcd.0",
		  "columns": [
		    {
		      "name": "col0", "type": "varchar", "typeSignature": { "rawType": "varchar", "typeArguments": [], "literalArguments": [] }
		    }
		  ],
		  "data": [
		    [ "c0r3" ],
		    [ "c0r4" ],
		    [ "c0r5" ]
		  ]
		}`, r.Host))
	default:
		http.NotFound(w, r)
	}
})

func TestRowsFetchBasic(t *testing.T) {
	ts := httptest.NewServer(oneRowColResponse)
	defer ts.Close()

	r := &rows{
		conn: &conn{
			client: http.DefaultClient,
		},
		nextURI: ts.URL + "/v1/query/abcd/1",
	}

	err := r.fetch()
	if err != nil {
		t.Fatal(err.Error())
	}

	cols := r.Columns()
	if len(cols) != 1 {
		t.Fatalf("got %d cols, wanted %d", len(cols), 1)
	}
	if cols[0] != "col0" {
		t.Errorf("got %v, wanted %v", cols[0], "col0")
	}

	values := make([]driver.Value, len(cols))

	if err := r.Next(values); err != nil {
		t.Fatal(err.Error())
	}

	if err := r.Next(values); err != io.EOF {
		t.Fatalf("got %v, wanted io.EOF", err)
	}
}

func TestRowsFetchResultFail(t *testing.T) {
	ts := httptest.NewServer(failingQueryResult)
	defer ts.Close()

	r := &rows{
		conn: &conn{
			client: http.DefaultClient,
		},
		nextURI: ts.URL + "/v1/query/abcd/1",
	}

	err := r.fetch()
	if err == nil {
		t.Fatal("got no error, wanted one")
	}
}

func TestRowsFetchSupportedTypes(t *testing.T) {
	ts := httptest.NewServer(supportedDatatypesResponse)
	defer ts.Close()

	r := &rows{
		conn: &conn{
			client: http.DefaultClient,
		},
		nextURI: ts.URL + "/v1/query/abcd/1",
	}

	err := r.fetch()
	if err != nil {
		t.Fatal(err.Error())
	}

	cols := r.Columns()
	values := make([]driver.Value, len(cols))

	if err := r.Next(values); err != nil {
		t.Fatal(err.Error())
	}

	expected := []interface{}{"c0r0", int64(12345), float64(12.45), true}

	if len(values) != len(expected) {
		t.Fatalf("got %d values, wanted %d", len(values), len(expected))
	}
	for i := range expected {
		if values[i] != expected[i] {
			t.Errorf("col%d: got %#v, wanted %#v", i, values[i], expected[i])
		}
	}
}

func TestRowsColumnsPerformsFetch(t *testing.T) {
	ts := httptest.NewServer(oneRowColResponse)
	defer ts.Close()

	r := &rows{
		conn: &conn{
			client: http.DefaultClient,
		},
		nextURI: ts.URL + "/v1/query/abcd/1",
	}

	cols := r.Columns()
	if len(cols) != 1 {
		t.Fatalf("got %d cols, wanted %d", len(cols), 1)
	}
	if cols[0] != "col0" {
		t.Errorf("got %v, wanted %v", cols[0], "col0")
	}
}

func TestRowsNextPerformsFetch(t *testing.T) {
	ts := httptest.NewServer(oneRowColResponse)
	defer ts.Close()

	r := &rows{
		conn: &conn{
			client: http.DefaultClient,
		},
		nextURI: ts.URL + "/v1/query/abcd/1",
	}

	values := make([]driver.Value, 1)

	if err := r.Next(values); err != nil {
		t.Fatal(err.Error())
	}
	if values[0] != "c0r0" {
		t.Errorf("got %v, wanted %v", values[0], "c0r0")
	}
}

func TestRowsNextMultipleRows(t *testing.T) {
	ts := httptest.NewServer(multiRowResponse)
	defer ts.Close()

	r := &rows{
		conn: &conn{
			client: http.DefaultClient,
		},
		nextURI: ts.URL + "/v1/query/abcd/1",
	}

	values := make([]driver.Value, 1)

	// Fetch 6 rows
	for i := 0; i < 6; i++ {
		if err := r.Next(values); err != nil {
			t.Fatalf("row %d: %v", i, err.Error())
		}
	}

	if err := r.Next(values); err != io.EOF {
		t.Fatalf("got %v, wanted io.EOF", err)
	}
}

func TestRowsNextMultiplePages(t *testing.T) {
	ts := httptest.NewServer(multiPageResponse)
	defer ts.Close()

	r := &rows{
		conn: &conn{
			client: http.DefaultClient,
		},
		nextURI: ts.URL + "/v1/query/abcd/1",
	}

	values := make([]driver.Value, 1)

	// Fetch 6 rows
	for i := 0; i < 6; i++ {
		if err := r.Next(values); err != nil {
			t.Fatalf("row %d: %v", i, err.Error())
		}
	}

	if err := r.Next(values); err != io.EOF {
		t.Fatalf("got %v, wanted io.EOF", err)
	}
}
