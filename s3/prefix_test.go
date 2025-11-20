package s3

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/SiaFoundation/s3d/s3"
)

func TestPrefixMatch(t *testing.T) {
	// cheap helpers for hiding pointer strings, which are used to increase
	// information density in the test case table:
	s := func(v string) *string { return &v }

	for idx, tc := range []struct {
		key    string
		prefix *string
		delim  *string
		out    *string
		common bool
	}{
		{key: "foo/bar", prefix: s("foo"), delim: s("/"), out: s("foo/"), common: true},
		{key: "foo/bar", prefix: s("foo/ba"), delim: s("/"), out: s("foo/bar")},
		{key: "foo/bar", prefix: s("foo/ba/"), delim: s("/"), out: nil},
		{key: "foo/bar", prefix: s("/"), delim: s("/"), out: s("foo/"), common: true},

		// without a delimiter, it's just a prefix match:
		{key: "foo/bar", prefix: s("foo/b"), out: s("foo/b")},
		{key: "foo/bar", prefix: s("foo/"), out: s("foo/")},
		{key: "foo/bar", prefix: s("foo"), out: s("foo")},
		{key: "foo/bar", prefix: s("fo"), out: s("fo")},
		{key: "foo/bar", prefix: s("f"), out: s("f")},
		{key: "foo/bar", prefix: s("q"), out: nil},

		// this could be a source of trouble - does "no prefix" mean "match
		// everything" or "match nothing"? What about "empty prefix"? For now,
		// these cases simply document what the current algorithm is expected to
		// do, but this needs further exploration:
		{key: "foo/bar", prefix: nil, out: s("foo/bar")},
		{key: "foo/bar", prefix: s(""), out: s("")},
	} {
		t.Run("", func(t *testing.T) {
			prefix := s3.Prefix{
				HasPrefix:    tc.prefix != nil,
				HasDelimiter: tc.delim != nil,
				Prefix:       unwrapStr(tc.prefix),
				Delimiter:    unwrapStr(tc.delim),
			}

			match := Match(prefix, tc.key)
			if (tc.out == nil) != (match == nil) {
				t.Fatal("prefix match failed at index", idx)
			}
			if tc.out != nil {
				if *tc.out != match.MatchedPart {
					t.Fatal("prefix matched part failed at index", idx, *tc.out, "!=", match.MatchedPart)
				}
				if tc.common != match.CommonPrefix {
					t.Fatal("prefix common failed at index", idx)
				}
			}
		})
	}
}

func TestNewPrefix(t *testing.T) {
	s := func(in string) *string { return &in }

	for _, tc := range []struct {
		prefix, delim *string
		out           s3.Prefix
	}{
		{nil, nil, s3.Prefix{}},
		{s("foo"), nil, s3.Prefix{HasPrefix: true, Prefix: "foo"}},
		{nil, s("foo"), s3.Prefix{HasDelimiter: true, Delimiter: "foo"}},
		{s("foo"), s("bar"), s3.Prefix{HasPrefix: true, Prefix: "foo", HasDelimiter: true, Delimiter: "bar"}},
	} {
		t.Run("", func(t *testing.T) {
			exp := NewPrefix(tc.prefix, tc.delim)
			if !reflect.DeepEqual(tc.out, exp) {
				t.Fatal(tc.out, "!=", exp)
			}
		})
	}
}

func TestPrefixFilePrefix(t *testing.T) {
	s := func(v string) *string { return &v }

	for idx, tc := range []struct {
		p, d      *string
		ok        bool
		path, rem string
	}{
		{s("foo/bar"), s("/"), true, "foo", "bar"},
		{s("foo/bar/"), s("/"), true, "foo/bar", ""},
		{s("foo/bar/b"), s("/"), true, "foo/bar", "b"},
		{s("foo"), s("/"), true, "", "foo"},
		{s("foo/"), s("/"), true, "foo", ""},
		{s("/"), s("/"), true, "", ""},
		{s(""), s("/"), true, "", ""},

		{s(""), nil, false, "", ""},
		{s("foo"), nil, false, "", ""},
		{s("foo/bar"), nil, false, "", ""},
		{s("foo-bar"), s("-"), false, "", ""},
	} {
		t.Run(fmt.Sprintf("%d/(%s-%s)", idx, tc.path, tc.rem), func(t *testing.T) {
			prefix := NewPrefix(tc.p, tc.d)

			foundPath, foundRem, ok := FilePrefix(prefix)
			if tc.ok != ok {
				t.Fatal()
			} else if tc.ok {
				if tc.path != foundPath {
					t.Fatal("prefix path", tc.path, "!=", foundPath)
				}
				if tc.rem != foundRem {
					t.Fatal("prefix rem", tc.rem, "!=", foundRem)
				}
			}
		})
	}
}

func unwrapStr(v *string) string {
	if v != nil {
		return *v
	}
	return ""
}
