package main

import "testing"

func TestValidateServerConfig(t *testing.T) {
	valid := Config{
		ApiAddress:    "127.0.0.1:8000",
		AdminAddress:  "127.0.0.1:8001",
		AdminPassword: "password",
		Sia: Sia{
			DataShards:   10,
			ParityShards: 20,
		},
	}

	if err := validateServerConfig(valid); err != nil {
		t.Fatal(err)
	}

	// reject redundancy the SDK would fail on later
	invalid := []struct {
		dataShards   uint8
		parityShards uint8
	}{
		{0, 20},
		{10, 0},
		{10, 4},
		{10, 60},
		{200, 200},
	}

	for _, tc := range invalid {
		c := valid
		c.Sia.DataShards = tc.dataShards
		c.Sia.ParityShards = tc.parityShards
		if err := validateServerConfig(c); err == nil {
			t.Fatal("expected error", tc.dataShards, tc.parityShards)
		}
	}
}
