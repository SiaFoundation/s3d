package main

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"

	"github.com/SiaFoundation/s3d/sia"
	"go.uber.org/zap"
	"golang.org/x/term"
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

	// S3 contains S3 related configuration.
	S3 struct {
		HostBases []string `yaml:"hostBases"`
	}

	// Sia contains the configuration for the Sia backend.
	Sia struct {
		DiskUsageLimit uint64 `yaml:"diskUsageLimit"`
	}

	// Backups contains the configuration for database backups.
	Backups struct {
		// Directory is where backups are written and listed. Defaults to a
		// "backups" directory inside the data directory.
		Directory string `yaml:"directory"`
	}

	// Config contains the configuration for S3d.
	Config struct {
		ApiAddress string `yaml:"apiAddress"`
		// AdminAddress is the address the admin API is served on. It must
		// differ from the S3 API address.
		AdminAddress string `yaml:"adminAddress"`
		// AdminPassword is the password required to access the admin API via
		// HTTP Basic authentication. It must not be empty.
		AdminPassword string  `yaml:"adminPassword"`
		Directory     string  `yaml:"directory"`
		Log           Log     `yaml:"log"`
		Sia           Sia     `yaml:"sia"`
		S3            S3      `yaml:"s3"`
		Backups       Backups `yaml:"backups"`
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

func runConfigCmd(fp string) {
	fmt.Println("s3d Configuration Wizard")
	fmt.Println("This wizard will help you configure s3d for the first time.")
	fmt.Println("You can always change these settings with the config command or by editing the config file.")

	if fp == "" {
		fp = configPath()
	}
	fp, err := filepath.Abs(fp)
	checkFatalError("failed to get absolute path of config file", err)

	fmt.Println("")
	fmt.Printf("Config Location %q\n", fp)

	if _, err := os.Stat(fp); err == nil {
		if !promptYesNo(fmt.Sprintf("%q already exists. Would you like to overwrite it?", fp)) {
			return
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		checkFatalError("failed to check if config file exists", err)
	} else {
		// ensure the config directory exists
		checkFatalError("failed to create config directory", os.MkdirAll(filepath.Dir(fp), 0700))
	}

	fmt.Println("")
	setDataDirectory()

	setAdminAPI()

	setAdvancedConfig()

	setAccessKeyPairs()

	// write the config file
	f, err := os.Create(fp)
	checkFatalError("failed to create config file", err)
	defer f.Close()

	enc := yaml.NewEncoder(f)
	defer enc.Close()

	checkFatalError("failed to encode config file", enc.Encode(cfg))
	checkFatalError("failed to sync config file", f.Sync())
}

// ansiStyle wraps the output in ANSI escape codes if enabled.
func ansiStyle(style, output string) string {
	if cfg.Log.StdOut.EnableANSI {
		return fmt.Sprintf("\033[%sm%s\033[0m", style, output)
	}
	return output
}

func configPath() string {
	if str := os.Getenv(configFileEnvVar); str != "" {
		return str
	}

	switch runtime.GOOS {
	case "windows":
		return filepath.Join(os.Getenv("APPDATA"), "s3d", "s3d.yml")
	case "darwin":
		return filepath.Join(os.Getenv("HOME"), "Library", "Application Support", "s3d", "s3d.yml")
	case "linux", "freebsd", "openbsd":
		return filepath.Join(string(filepath.Separator), "etc", "s3d", "s3d.yml")
	default:
		return "s3d.yml"
	}
}

func humanList(s []string, sep string) string {
	if len(s) == 0 {
		return ""
	} else if len(s) == 1 {
		return fmt.Sprintf(`%q`, s[0])
	} else if len(s) == 2 {
		return fmt.Sprintf(`%q %s %q`, s[0], sep, s[1])
	}

	var sb strings.Builder
	for i, v := range s {
		if i != 0 {
			sb.WriteString(", ")
		}
		if i == len(s)-1 {
			sb.WriteString("or ")
		}
		sb.WriteString(`"`)
		sb.WriteString(v)
		sb.WriteString(`"`)
	}
	return sb.String()
}

func setAccessKeyPairs() {
	fmt.Println("")
	if !promptYesNo("Would you like to create an initial S3 user and access key now?") {
		fmt.Println("")
		fmt.Println("To create S3 access credentials later, run:")
		fmt.Println("  s3d users create <username>")
		fmt.Println("  s3d keys create <username>")
		return
	}

	store, err := openStore(zap.NewNop())
	checkFatalError("failed to open database", err)
	defer store.Close()

	var userName string
	for {
		userName = readInput("Enter user name")
		if userName == "" {
			stdoutError("User name must not be empty.")
			continue
		}
		err := store.CreateUser(userName)
		if err == nil {
			break
		} else if errors.Is(err, sia.ErrUserAlreadyExists) {
			fmt.Printf("User %q already exists, reusing it.\n", userName)
			break
		}
		checkFatalError("failed to create user", err)
	}

	accessKey, secretKey := generateAccessKey()
	checkFatalError("failed to create access key", store.CreateAccessKey(userName, accessKey, secretKey))

	fmt.Println("")
	fmt.Printf("  Access Key: %s\n", accessKey)
	fmt.Printf("  Secret Key: %s\n", secretKey)
	fmt.Println("")
	fmt.Println(ansiStyle("1", "Save these credentials. The secret key will not be shown again."))
}

func setAdvancedConfig() {
	fmt.Println("")
	if !promptYesNo("Would you like to configure advanced settings?") {
		return
	}

	fmt.Println("")
	fmt.Println("Advanced settings are used to configure S3d's behavior.")
	fmt.Println("You can leave these settings blank to use the defaults.")
	fmt.Println("")

	// http address of the S3 API
	fmt.Println("The HTTP address is used to serve the S3 API.")
	fmt.Println("It should only be exposed to the public internet via an https reverse proxy")
	setListenAddress("HTTP Address", &cfg.ApiAddress)
}

func setAdminAPI() {
	fmt.Println("")
	fmt.Println("The admin API serves Prometheus metrics about the upload pipeline on a separate HTTP address.")

	if cfg.AdminAddress == "" {
		cfg.AdminAddress = "127.0.0.1:8001"
	}
	for {
		setListenAddress("Admin API Address", &cfg.AdminAddress)
		if cfg.AdminAddress != cfg.ApiAddress {
			break
		}
		stdoutError("The admin API address must differ from the S3 API address.")
	}

	for {
		prompt := "Enter admin API password"
		if cfg.AdminPassword != "" {
			prompt += " (leave blank to keep the current password)"
		}
		password := readPasswordInput(prompt)
		if password != "" {
			cfg.AdminPassword = password
			return
		} else if cfg.AdminPassword != "" {
			return
		}
		stdoutError("The admin API password must not be empty.")
	}
}

func setListenAddress(context string, value *string) {
	// will continue to prompt until a valid value is entered
	for {
		input := readInput(fmt.Sprintf("%s (currently %q)", context, *value))
		if input == "" {
			return
		}

		host, port, err := net.SplitHostPort(input)
		if err != nil {
			stdoutError(fmt.Sprintf("Invalid %s port %q: %s", context, input, err.Error()))
			continue
		}

		n, err := strconv.Atoi(port)
		if err != nil {
			stdoutError(fmt.Sprintf("Invalid %s port %q: %s", context, input, err.Error()))
			continue
		} else if n < 0 || n > 65535 {
			stdoutError(fmt.Sprintf("Invalid %s port %q: must be between 0 and 65535", context, input))
			continue
		}
		*value = net.JoinHostPort(host, port)
		return
	}
}

func readInput(context string) string {
	fmt.Printf("%s: ", context)
	r := bufio.NewReader(os.Stdin)
	input, err := r.ReadString('\n')
	checkFatalError("failed to read input", err)
	return strings.TrimSpace(input)
}

// readPasswordInput reads a password from stdin.
func readPasswordInput(context string) string {
	fmt.Printf("%s: ", context)
	input, err := term.ReadPassword(int(os.Stdin.Fd()))
	checkFatalError("failed to read password input", err)
	fmt.Println("")
	return string(input)
}

func promptQuestion(question string, answers []string) string {
	for {
		input := readInput(fmt.Sprintf("%s (%s)", question, strings.Join(answers, "/")))
		for _, answer := range answers {
			if strings.EqualFold(input, answer) {
				return answer
			}
		}
		fmt.Println(ansiStyle("31", fmt.Sprintf("Answer must be %s", humanList(answers, "or"))))
	}
}

func promptYesNo(question string) bool {
	answer := promptQuestion(question, []string{"yes", "no"})
	return strings.EqualFold(answer, "yes")
}

func setDataDirectory() {
	if cfg.Directory == "" {
		cfg.Directory = "."
	}

	dir, err := filepath.Abs(cfg.Directory)
	checkFatalError("failed to get absolute path of data directory", err)

	fmt.Println("The data directory is where s3d will store its metadata.")
	fmt.Println("This directory should be on a fast, reliable storage device, preferably an SSD.")
	fmt.Println("")

	_, existsErr := os.Stat(filepath.Join(cfg.Directory, "s3d.db"))
	dataExists := existsErr == nil
	if dataExists {
		fmt.Println(ansiStyle("33", "There is existing data in the data directory."))
		fmt.Println(ansiStyle("33", "If you change your data directory, you will need to manually move the data from your old data directory to your new one."))
	}

	if !promptYesNo("Would you like to change the data directory? (Current: " + dir + ")") {
		return
	}
	cfg.Directory = readInput("Enter data directory")
}

// stdoutError prints an error message to stdout
func stdoutError(msg string) {
	if cfg.Log.StdOut.EnableANSI {
		fmt.Println(ansiStyle("31", msg))
	} else {
		fmt.Println(msg)
	}
}
