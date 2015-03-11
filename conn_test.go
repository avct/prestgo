package prestgo

import (
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
