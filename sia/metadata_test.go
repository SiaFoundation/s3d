package sia

import (
	"crypto/md5"
	"reflect"
	"testing"
)

func TestMetadata(t *testing.T) {
	meta := objectMeta{
		contentMD5: md5.Sum([]byte("hello world")),
		meta: map[string]string{
			"a": "b",
			"c": "d",
		},
	}

	d, err := meta.encode()
	if err != nil {
		t.Fatal(err)
	}

	var meta2 objectMeta
	if err := meta2.decode(d); err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(meta, meta2) {
		t.Fatalf("metadata mismatch: expected %+v, got %+v", meta, meta2)
	}
}
