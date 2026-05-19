package s3

import (
	"encoding/xml"
	"strings"
	"testing"
)

func TestStorageClassXML(t *testing.T) {
	// verify that an empty StorageClass defaults to STANDARD
	var sc StorageClass
	buf, err := xml.Marshal(sc)
	if err != nil {
		t.Fatal(err)
	} else if !strings.Contains(string(buf), "STANDARD") {
		t.Fatalf("expected STANDARD, got %q", string(buf))
	}

	// verify that Content always includes StorageClass in XML output,
	// this was a regression where omitempty caused the element to be
	// absent breaking clients like Arq Backup
	c := Content{
		Key:  "test.txt",
		Size: 1024,
	}
	buf, err = xml.Marshal(c)
	if err != nil {
		t.Fatal(err)
	} else if !strings.Contains(string(buf), "<StorageClass>STANDARD</StorageClass>") {
		t.Fatalf("expected <StorageClass>STANDARD</StorageClass> in XML output, got %q", string(buf))
	}
}
