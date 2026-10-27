package auth_test

import (
	"strings"
	"testing"

	"github.com/SiaFoundation/s3d/s3/auth"
)

func TestGenerateAccessKey(t *testing.T) {
	accessKeyID, secretKey := auth.GenerateAccessKey()
	if len(accessKeyID) != 20 || len(secretKey) != 40 {
		t.Fatalf("unexpected lengths %d and %d", len(accessKeyID), len(secretKey))
	} else if err := auth.ValidateAccessKey(accessKeyID, secretKey); err != nil {
		t.Fatal(err)
	}

	// the ID is split out of the SigV4 credential on slashes and the header on
	// commas, with spaces stripped beforehand
	invalid := [][2]string{
		{strings.Repeat("a", 15), secretKey},
		{strings.Repeat("a", 129), secretKey},
		{"AAAAAAAAA/AAAAAAAAAA", secretKey},
		{"AAAAAAAAA,AAAAAAAAAA", secretKey},
		{"AAAAAAAAA AAAAAAAAAA", secretKey},
		{accessKeyID, strings.Repeat("a", 31)},
		{accessKeyID, strings.Repeat("a", 129)},
	}
	for _, pair := range invalid {
		if err := auth.ValidateAccessKey(pair[0], pair[1]); err == nil {
			t.Fatalf("expected %q to be rejected", pair)
		}
	}
}
