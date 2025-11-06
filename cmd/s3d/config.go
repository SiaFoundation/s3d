package main

import (
	"bytes"
	"fmt"
	"os"

	"go.uber.org/zap"
	"gopkg.in/yaml.v3"
)

type (
	// FileLog configures the file output of the logger.
	FileLog struct {
		Enabled bool            `yaml:"enabled"`
		Level   zap.AtomicLevel `yaml:"level"`
		Format  string          `yaml:"format"`
		// Path is the path of the log file.
		Path string `yaml:"path"`
	}

	// StdOutLog configures the standard output of the logger.
	StdOutLog struct {
		Level      zap.AtomicLevel `yaml:"level"`
		Enabled    bool            `yaml:"enabled"`
		Format     string          `yaml:"format"`
		EnableANSI bool            `yaml:"enableANSI"` //nolint:tagliatelle
	}
	// Log contains the configuration for the logger.
	Log struct {
		StdOut StdOutLog `yaml:"stdout"`
		File   FileLog   `yaml:"file"`
	}

	S3 struct {
		AccessKey string   `yaml:"accessKey"`
		SecretKey string   `yaml:"secretKey"`
		HostBases []string `yaml:"hostBases"`
	}

	V4KeyPair struct {
		AccessKey string `yaml:"accessKey"`
		SecretKey string `yaml:"secretKey"`
	}

	// Config contains the configuration for the indexer
	Config struct {
		ApiAddress string `yaml:"apiAddress"`
		AppSecret  string `yaml:"appSecret"`
		Directory  string `yaml:"directory"`
		Log        Log    `yaml:"log"`
		S3         S3     `yaml:"sia"`
	}
)

// LoadFile loads the configuration from the provided file path.
// If the file does not exist or cannot be decoded, an error is returned.
func LoadFile(fp string, cfg *Config) error {
	buf, err := os.ReadFile(fp)
	if err != nil {
		return fmt.Errorf("failed to read config file: %w", err)
	}

	r := bytes.NewReader(buf)
	dec := yaml.NewDecoder(r)
	dec.KnownFields(true)

	return dec.Decode(cfg)
}
