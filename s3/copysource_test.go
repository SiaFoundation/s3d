package s3

import "testing"

func TestParseSource(t *testing.T) {
	tests := []struct {
		source  string
		bucket  string
		object  string
		version VersionRequest
		wantErr bool
	}{
		{source: "bucket/key", bucket: "bucket", object: "key"},
		{source: "/bucket/key", bucket: "bucket", object: "key"},
		{source: "bucket/dir/file", bucket: "bucket", object: "dir/file"},

		// some clients encode the whole source, separator included
		{source: "bucket%2Fkey", bucket: "bucket", object: "key"},
		{source: "bucket/dir%2Ffile", bucket: "bucket", object: "dir/file"},
		{source: "bucket%2Fkey?versionId=v1", bucket: "bucket", object: "key", version: SpecificVersion("v1")},

		// the source is a path, so a plus is literal rather than a space
		{source: "bucket/a+b", bucket: "bucket", object: "a+b"},

		{source: "bucket", wantErr: true},
		{source: "bucket/100%off", wantErr: true},
	}

	for _, tt := range tests {
		bucket, object, version, err := parseSource(tt.source)
		if tt.wantErr {
			if err == nil {
				t.Fatal("expected error", tt.source)
			}
			continue
		}

		if err != nil {
			t.Fatal(tt.source, err)
		} else if bucket != tt.bucket {
			t.Fatal("bucket mismatch", tt.source, bucket)
		} else if object != tt.object {
			t.Fatal("object mismatch", tt.source, object)
		} else if version != tt.version {
			t.Fatal("version mismatch", tt.source, version)
		}
	}
}
