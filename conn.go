package prestgo

import (
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Name of the driver to use when calling `sql.Open`
const DriverName = "prestgo"

// Default data source parameters
const (
	DefaultPort    = "8080"
	DefaultCatalog = "hive"
	DefaultSchema  = "default"
)

var (
	// ErrNotSupported is returned when an unsupported feature is requested.
	ErrNotSupported = errors.New(DriverName + ": not supported")

	// ErrQueryFailed indicates that a network or server failure prevented the driver obtaining a query result.
	ErrQueryFailed = errors.New(DriverName + ": query failed")
)

func init() {
	sql.Register(DriverName, &drv{})
}

type drv struct{}

func (*drv) Open(name string) (driver.Conn, error) {
	return Open(name)
}

// Open creates a connection to the specified data source name which should be
// of the form "presto://hostname:port/catalog/schema". http.DefaultClient will
// be used for communicationg with the Presto server.
func Open(name string) (driver.Conn, error) {
	return ClientOpen(http.DefaultClient, name)
}

// ClientOpen creates a connection to the specified data source name using the supplied
// HTTP client. The data source name should be of the form
// "presto://hostname:port/catalog/schema".
func ClientOpen(client *http.Client, name string) (driver.Conn, error) {

	conf := make(config)
	conf.parseDataSource(name)

	cn := &conn{
		client:  client,
		addr:    conf["addr"],
		catalog: conf["catalog"],
		schema:  conf["schema"],
	}
	return cn, nil
}

type conn struct {
	client  *http.Client
	addr    string
	catalog string
	schema  string
}

var _ driver.Conn = &conn{}

func (c *conn) Prepare(query string) (driver.Stmt, error) {
	st := &stmt{
		conn:  c,
		query: query,
	}
	return st, nil
}

func (c *conn) Close() error {
	return nil
}

func (c *conn) Begin() (driver.Tx, error) {
	return nil, ErrNotSupported
}

type stmt struct {
	conn  *conn
	query string
}

var _ driver.Stmt = &stmt{}

func (s *stmt) Close() error {
	return nil
}

func (s *stmt) NumInput() int {
	return -1 // TODO: parse query for parameters
}

func (s *stmt) Exec(args []driver.Value) (driver.Result, error) {
	return nil, ErrNotSupported
}

func (s *stmt) Query(args []driver.Value) (driver.Rows, error) {
	// TODO: support query argument substitution
	if len(args) > 0 {
		return nil, ErrNotSupported
	}
	queryURL := fmt.Sprintf("http://%s/v1/statement", s.conn.addr)

	req, err := http.NewRequest("POST", queryURL, strings.NewReader(s.query))
	if err != nil {
		return nil, err
	}
	req.Header.Add("X-Presto-User", "default")
	req.Header.Add("X-Presto-Catalog", s.conn.catalog)
	req.Header.Add("X-Presto-Schema", s.conn.schema)

	resp, err := s.conn.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	// Presto doesn't use the http response code, parse errors come back as 200
	if resp.StatusCode != 200 {
		return nil, ErrQueryFailed
	}

	var sresp stmtResponse
	err = json.NewDecoder(resp.Body).Decode(&sresp)
	if err != nil {
		return nil, err
	}

	if sresp.Stats.State == "FAILED" {
		return nil, sresp.Error
	}

	r := &rows{
		conn:    s.conn,
		nextURI: sresp.NextURI,
	}

	return r, nil
}

type rows struct {
	conn     *conn
	nextURI  string
	fetched  bool
	rowindex int
	columns  []string
	types    []driver.ValueConverter
	data     []queryData
}

var _ driver.Rows = &rows{}

func (r *rows) fetch() error {
	nextReq, err := http.NewRequest("GET", r.nextURI, nil)
	if err != nil {
		return err
	}

	nextResp, err := r.conn.client.Do(nextReq)
	if err != nil {
		return err
	}

	if nextResp.StatusCode != 200 {
		nextResp.Body.Close()
		return ErrQueryFailed
	}

	var qresp queryResponse
	err = json.NewDecoder(nextResp.Body).Decode(&qresp)
	nextResp.Body.Close()
	if err != nil {
		return err
	}

	if qresp.Stats.State == "FAILED" {
		return qresp.Error
	}

	r.rowindex = 0
	r.data = qresp.Data

	// Note: qresp.Stats.State will be FINISHED when last page is retrieved
	r.nextURI = qresp.NextURI

	if !r.fetched {
		r.fetched = true
		r.columns = make([]string, len(qresp.Columns))
		r.types = make([]driver.ValueConverter, len(qresp.Columns))
		for i, col := range qresp.Columns {
			r.columns[i] = col.Name
			switch col.Type {
			case VarChar:
				r.types[i] = driver.String
			case BigInt:
				r.types[i] = bigIntConverter
			case Boolean:
				r.types[i] = driver.Bool
			case Double:
				r.types[i] = doubleConverter
			case Timestamp:
				r.types[i] = timestampConverter

			default:
				return fmt.Errorf("unsupported column type: %s", col.Type)
			}
		}
	}

	if len(qresp.Data) == 0 {
		return io.EOF
	}

	return nil
}

func (r *rows) Columns() []string {
	if !r.fetched {
		r.fetch()
	}
	return r.columns
}

func (r *rows) Close() error {
	return nil
}

func (r *rows) Next(dest []driver.Value) error {
	if !r.fetched || r.rowindex >= len(r.data) {
		if r.nextURI == "" {
			return io.EOF
		}
		if err := r.fetch(); err != nil {
			return err
		}
	}

	for i, v := range r.types {
		val, err := v.ConvertValue(r.data[r.rowindex][i])
		if err != nil {
			return err // TODO: more context in error
		}
		dest[i] = val
	}
	r.rowindex++
	return nil
}

type config map[string]string

func (c config) parseDataSource(ds string) error {
	u, err := url.Parse(ds)
	if err != nil {
		return err
	}

	if strings.IndexRune(u.Host, ':') == -1 {
		c["addr"] = u.Host + ":" + DefaultPort
	} else {
		c["addr"] = u.Host
	}

	c["catalog"] = DefaultCatalog
	c["schema"] = DefaultSchema

	pathSegments := strings.FieldsFunc(u.Path, func(c rune) bool { return c == '/' })
	if len(pathSegments) > 0 {
		c["catalog"] = pathSegments[0]
	}
	if len(pathSegments) > 1 {
		c["schema"] = pathSegments[1]
	}
	return nil
}

type valueConverterFunc func(v interface{}) (driver.Value, error)

func (fn valueConverterFunc) ConvertValue(v interface{}) (driver.Value, error) {
	return fn(v)
}

// bigIntConverter converts a value from the underlying json response into an int64.
// The Go JSON decoder uses float64 for generic numeric values
var bigIntConverter = valueConverterFunc(func(val interface{}) (driver.Value, error) {
	if vv, ok := val.(float64); ok {
		return int64(vv), nil
	}
	return nil, fmt.Errorf("%s: failed to convert %v (%T) into type int64", DriverName, val, val)
})

// doubleConverter converts a value from the underlying json response into an int64.
// The Go JSON decoder uses float64 for generic numeric values
var doubleConverter = valueConverterFunc(func(val interface{}) (driver.Value, error) {
	if vv, ok := val.(float64); ok {
		return vv, nil
	}
	return nil, fmt.Errorf("%s: failed to convert %v (%T) into type float64", DriverName, val, val)
})

// timestampConverter converts a value from the underlying json response into a time.Time.
var timestampConverter = valueConverterFunc(func(val interface{}) (driver.Value, error) {
	if vv, ok := val.(string); ok {
		// BUG: should parse using session time zone.
		ts, err := time.ParseInLocation("2006-01-02 15:04:05.000", vv, time.Local)
		return ts, err
	}
	return nil, fmt.Errorf("%s: failed to convert %v (%T) into type time.Time", DriverName, val, val)
})
