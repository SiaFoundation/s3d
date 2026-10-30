package s3

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/SiaFoundation/s3d/s3/s3errs"
)

func TestLifecycleFilterXMLTags(t *testing.T) {
	const project = `<Tag><Key>project</Key><Value>one</Value></Tag>`
	const temporary = `<Tag><Key>temporary</Key><Value>true</Value></Tag>`
	for _, test := range []struct {
		name   string
		filter string
		tags   TagSet
		valid  bool
	}{
		{"empty", "", nil, true},
		{"single tag", project, TagSet{{Key: "project", Value: "one"}}, true},
		{"sibling tags", project + temporary, nil, false},
		{"repeated tag", project + project, nil, false},
		{"tags inside And", "<And>" + project + temporary + "</And>", TagSet{{Key: "project", Value: "one"}, {Key: "temporary", Value: "true"}}, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			body := `<LifecycleConfiguration><Rule><Status>Enabled</Status><Filter>` + test.filter + `</Filter><Expiration><Days>1</Days></Expiration></Rule></LifecycleConfiguration>`
			var config LifecycleConfiguration
			err := decodeXMLBody(strings.NewReader(body), &config)
			if !test.valid {
				if !errors.Is(err, s3errs.ErrMalformedXML) {
					t.Fatalf("expected MalformedXML, got %v", err)
				}
				return
			} else if err != nil {
				t.Fatal(err)
			} else if err := config.Validate(); err != nil {
				t.Fatal(err)
			}
			if got := config.Rules[0].EffectiveTags(); !reflect.DeepEqual(got, test.tags) {
				t.Fatalf("tags: got %v, want %v", got, test.tags)
			}
		})
	}
}
