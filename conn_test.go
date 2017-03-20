package prestgo

import (
	"database/sql/driver"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"
)

func TestConfigParseDataSource(t *testing.T) {
	testCases := []struct {
		ds       string
		expected config
		error    bool
	}{

		{
			ds:       "",
			expected: config{"addr": ":8080", "catalog": "hive", "schema": "default", "user": "prestgo"},
			error:    false,
		},

		{
			ds:       "presto://example:9000/",
			expected: config{"addr": "example:9000", "catalog": "hive", "schema": "default", "user": "prestgo"},
			error:    false,
		},

		{
			ds:       "presto://example/",
			expected: config{"addr": "example:8080", "catalog": "hive", "schema": "default", "user": "prestgo"},
			error:    false,
		},

		{
			ds:       "presto://example/tree",
			expected: config{"addr": "example:8080", "catalog": "tree", "schema": "default", "user": "prestgo"},
			error:    false,
		},

		{
			ds:       "presto://example/tree/",
			expected: config{"addr": "example:8080", "catalog": "tree", "schema": "default", "user": "prestgo"},
			error:    false,
		},

		{
			ds:       "presto://example/tree/birch",
			expected: config{"addr": "example:8080", "catalog": "tree", "schema": "birch", "user": "prestgo"},
			error:    false,
		},

		{
			ds:       "presto://name@example/",
			expected: config{"addr": "example:8080", "catalog": "hive", "schema": "default", "user": "name"},
			error:    false,
		},

		{
			ds:       "presto://name:pwd@example/",
			expected: config{"addr": "example:8080", "catalog": "hive", "schema": "default", "user": "name"},
			error:    false,
		},

		{
			ds:       "presto://name@example:9000/",
			expected: config{"addr": "example:9000", "catalog": "hive", "schema": "default", "user": "name"},
			error:    false,
		},

		{
			ds:       "presto://name:pwd@example:9000/",
			expected: config{"addr": "example:9000", "catalog": "hive", "schema": "default", "user": "name"},
			error:    false,
		},

		{
			ds:       "presto://name@example/tree/birch",
			expected: config{"addr": "example:8080", "catalog": "tree", "schema": "birch", "user": "name"},
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
		    { "name": "col3", "type": "boolean", "typeSignature": { "rawType": "varchar", "typeArguments": [], "literalArguments": [] } },
		    { "name": "col4", "type": "timestamp", "typeSignature": { "rawType": "varchar", "typeArguments": [], "literalArguments": [] } },
		    { "name": "col5", "type": "integer", "typeSignature": { "rawType": "integer", "typeArguments": [], "literalArguments": [] } }
		  ],
		  "data": [
		    [ "c0r0", 12345, 12.45, true, "2015-02-09 18:26:02.013", 12 ]
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

	expected := []interface{}{"c0r0", int64(12345), float64(12.45), true, time.Date(2015, 2, 9, 18, 26, 02, 13000000, time.Local), int64(12)}

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

func TestDoubleConverter(t *testing.T) {
	testCases := []struct {
		val      interface{}
		expected driver.Value
		err      bool
	}{

		{
			val:      0.91,
			expected: driver.Value(0.91),
			err:      false,
		},

		{
			val:      "foo",
			expected: nil,
			err:      true,
		},

		{
			val:      "Infinity",
			expected: math.Inf(1),
			err:      false,
		},

		{
			val:      "NaN",
			expected: math.NaN(),
			err:      false,
		},

		{
			val:      nil,
			expected: nil,
			err:      false,
		},
	}

	for _, tc := range testCases {
		v, err := doubleConverter(tc.val)

		if tc.err == (err == nil) {
			t.Errorf("%v: got error %v, wanted %v", tc.val, err, tc.err)
		}

		if ef, ok := tc.expected.(float64); ok && math.IsNaN(ef) {
			vf, ok := v.(float64)
			if !ok {
				t.Errorf("%v: got type %T, wanted a float64", tc.val, v)
				continue
			}

			if !math.IsNaN(vf) {
				t.Errorf("%v: wanted NaN", tc.val)
			}
			continue
		}

		if v != tc.expected {
			t.Errorf("%v: got %v, wanted %v", tc.val, v, tc.expected)
		}

	}
}

func TestBigIntConverter(t *testing.T) {
	testCases := []struct {
		val      interface{}
		expected driver.Value
		err      bool
	}{

		{
			val:      1000.0,
			expected: driver.Value(int64(1000)),
			err:      false,
		},

		{
			val:      "foo",
			expected: nil,
			err:      true,
		},

		{
			val:      "Infinity",
			expected: nil,
			err:      true,
		},

		{
			val:      "NaN",
			expected: nil,
			err:      true,
		},

		{
			val:      nil,
			expected: nil,
			err:      false,
		},
	}

	for _, tc := range testCases {
		v, err := bigIntConverter(tc.val)

		if tc.err == (err == nil) {
			t.Errorf("%v: got error %v, wanted %v", tc.val, err, tc.err)
		}

		if v != tc.expected {
			t.Errorf("%v: got %v, wanted %v", tc.val, v, tc.expected)
		}

	}
}

func TestTimestampConverter(t *testing.T) {
	testCases := []struct {
		val      interface{}
		expected driver.Value
		err      bool
	}{

		{
			val:      "2015-04-23 10:00:08.123",
			expected: time.Date(2015, 04, 23, 10, 0, 8, int(123*time.Millisecond), time.Local),
			err:      false,
		},

		{
			val:      1000.0,
			expected: nil,
			err:      true,
		},

		{
			val:      "foo",
			expected: nil,
			err:      true,
		},

		{
			val:      "Infinity",
			expected: nil,
			err:      true,
		},

		{
			val:      "NaN",
			expected: nil,
			err:      true,
		},

		{
			val:      nil,
			expected: nil,
			err:      false,
		},
	}

	for _, tc := range testCases {
		v, err := timestampConverter(tc.val)

		if tc.err == (err == nil) {
			t.Errorf("%v: got error %v, wanted %v", tc.val, err, tc.err)
		}

		if v != tc.expected {
			t.Errorf("%v: got %v, wanted %v", tc.val, v, tc.expected)
		}

	}
}

func TestTimestampWithTimezoneConverter(t *testing.T) {
	europeLondon, err := time.LoadLocation("Europe/London")
	if err != nil {
		t.Fatal(err)
	}

	testCases := []struct {
		val      interface{}
		expected driver.Value
		err      bool
	}{

		{
			val:      "2015-04-23 10:00:08.123 UTC",
			expected: time.Date(2015, 04, 23, 10, 0, 8, int(123*time.Millisecond), time.UTC),
			err:      false,
		},

		{
			val:      "2015-04-23 10:00:08.123 Europe/London",
			expected: time.Date(2015, 04, 23, 10, 0, 8, int(123*time.Millisecond), europeLondon),
			err:      false,
		},

		{
			val:      "2015-04-23 10:00:08.123",
			expected: time.Date(2015, 04, 23, 10, 0, 8, int(123*time.Millisecond), time.Local),
			err:      false,
		},

		{
			val:      "2015-04-23 10:00:08.123 ",
			expected: time.Date(2015, 04, 23, 10, 0, 8, int(123*time.Millisecond), time.UTC),
			err:      false,
		},

		{
			val:      "2015-04-23 10:00:08.123 Nowhere",
			expected: nil,
			err:      true,
		},

		{
			val:      1000.0,
			expected: nil,
			err:      true,
		},

		{
			val:      "foo",
			expected: nil,
			err:      true,
		},

		{
			val:      "Infinity",
			expected: nil,
			err:      true,
		},

		{
			val:      "NaN",
			expected: nil,
			err:      true,
		},

		{
			val:      nil,
			expected: nil,
			err:      false,
		},
	}

	for _, tc := range testCases {
		v, err := timestampWithTimezoneConverter(tc.val)

		if tc.err == (err == nil) {
			t.Errorf("%v: got error %v, wanted %v", tc.val, err, tc.err)
		}

		if !reflect.DeepEqual(v, tc.expected) {
			t.Errorf("%v: got %v, wanted %v", tc.val, v, tc.expected)
		}

	}
}

func TestVarBinaryConverter(t *testing.T) {
	testCases := []struct {
		val      interface{}
		expected driver.Value
		err      bool
	}{
		{
			val:      "AAAAAAAAAAAAAP//2V9/MQ==",
			expected: []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 255, 255, 217, 95, 127, 49},
			err:      false,
		},
		{
			val:      "AAAAAAAAAAAAAP//2V9/MQ==InvalidBase64!",
			expected: nil,
			err:      true,
		},
		{
			val:      1000.0,
			expected: nil,
			err:      true,
		},
		{
			val:      nil,
			expected: nil,
			err:      false,
		},
	}

	for _, tc := range testCases {
		v, err := varbinaryConverter(tc.val)

		if tc.err == (err == nil) {
			t.Errorf("%v: got error %v, wanted %v", tc.val, err, tc.err)
		}

		if !reflect.DeepEqual(v, tc.expected) {
			t.Errorf("%v: got %v, wanted %v", tc.val, v, tc.expected)
		}
	}
}

func TestMapVarcharConverter(t *testing.T) {
	testCases := []struct {
		val      interface{}
		expected driver.Value
		err      bool
	}{
		{
			val:      map[string]interface{}{"testKey": "testVal"},
			expected: map[string]string{"testKey": "testVal"},
			err:      false,
		},
		{
			val:      "InvalidMap",
			expected: nil,
			err:      true,
		},
		{
			val:      nil,
			expected: nil,
			err:      false,
		},
	}

	for _, tc := range testCases {
		v, err := mapVarcharConverter(tc.val)

		if tc.err == (err == nil) {
			t.Errorf("%v: got error %v, wanted %v", tc.val, err, tc.err)
		}

		if !reflect.DeepEqual(v, tc.expected) {
			t.Errorf("%v: got %v, wanted %v", tc.val, v, tc.expected)
		}

	}
}

func TestArrayVarcharConverter(t *testing.T) {
	testCases := []struct {
		val      interface{}
		expected driver.Value
		err      bool
	}{
		{
			val:      []interface{}{"testVal1", "testVal2"},
			expected: []string{"testVal1", "testVal2"},
			err:      false,
		},
		{
			val:      []interface{}{1, 2},
			expected: nil,
			err:      true,
		},
		{
			val:      "InvalidArray",
			expected: nil,
			err:      true,
		},
		{
			val:      nil,
			expected: nil,
			err:      false,
		},
	}

	for _, tc := range testCases {
		v, err := arrayVarcharConverter(tc.val)

		if tc.err == (err == nil) {
			t.Errorf("%v: got error %v, wanted %v", tc.val, err, tc.err)
		}

		if !reflect.DeepEqual(v, tc.expected) {
			t.Errorf("%v: got %v, wanted %v", tc.val, v, tc.expected)
		}

	}
}
