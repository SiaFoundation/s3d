package sqlite

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"time"

	"go.sia.tech/core/types"
)

var (
	_ scannerValuer = (*sqlHash256)(nil)
	_ scannerValuer = (*sqlMD5)(nil)
	_ scannerValuer = (*sqlTime)(nil)
	_ scannerValuer = (*sqlUploadID)(nil)
)

type scannerValuer interface {
	driver.Valuer
	sql.Scanner
}

type sqlHash256 types.Hash256

func (h *sqlHash256) Scan(src any) error {
	switch src := src.(type) {
	case []byte:
		if len(src) != len(sqlHash256{}) {
			return fmt.Errorf("failed to scan source into Hash256 due to invalid number of bytes %v != %v: %v", len(src), len(sqlHash256{}), src)
		}
		copy(h[:], src)
		return nil
	default:
		return fmt.Errorf("cannot scan %T to Hash256", src)
	}
}

func (h sqlHash256) Value() (driver.Value, error) {
	return h[:], nil
}

type sqlTime time.Time

func (t *sqlTime) Scan(src any) error {
	switch src := src.(type) {
	case int64:
		*t = sqlTime(time.UnixMilli(src))
		return nil
	default:
		return fmt.Errorf("cannot scan %T to time.Time", src)
	}
}

func (t sqlTime) Value() (driver.Value, error) {
	return time.Time(t).UnixMilli(), nil
}

type sqlMD5 [16]byte

func (m *sqlMD5) Scan(src any) error {
	switch src := src.(type) {
	case []byte:
		if len(src) != len(sqlMD5{}) {
			return fmt.Errorf("failed to scan source into MD5 due to invalid number of bytes %v != %v: %v", len(src), len(sqlMD5{}), src)
		}
		copy(m[:], src)
		return nil
	default:
		return fmt.Errorf("cannot scan %T to MD5", src)
	}
}

func (m sqlMD5) Value() (driver.Value, error) {
	return m[:], nil
}

type sqlUploadID [16]byte

func (uid *sqlUploadID) Scan(src any) error {
	switch src := src.(type) {
	case []byte:
		if len(src) != len(sqlUploadID{}) {
			return fmt.Errorf("failed to scan source into UploadID due to invalid number of bytes %v != %v: %v", len(src), len(sqlUploadID{}), src)
		}
		copy(uid[:], src)
		return nil
	default:
		return fmt.Errorf("cannot scan %T to UploadID", src)
	}
}

func (uid sqlUploadID) Value() (driver.Value, error) {
	return uid[:], nil
}
