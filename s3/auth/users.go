package auth

import (
	"fmt"
	"regexp"
)

const maxUserNameLength = 64

var userNamePattern = regexp.MustCompile(`^[\w+=,.@-]+$`)

// ValidateUserName checks that a user name has an acceptable length and
// matches the AWS IAM user name pattern.
func ValidateUserName(name string) error {
	if name == "" || len(name) > maxUserNameLength {
		return fmt.Errorf("user name must be between 1 and %d characters", maxUserNameLength)
	} else if !userNamePattern.MatchString(name) {
		return fmt.Errorf("user name must contain only ASCII letters, digits or the characters +=,.@-_")
	}
	return nil
}
