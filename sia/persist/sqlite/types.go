package sqlite

import (
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"time"

	"github.com/SiaFoundation/s3d/s3"
	"go.sia.tech/core/types"
)

var (
	_ scannerValuer = (*sqlHash256)(nil)
	_ scannerValuer = (*sqlMD5)(nil)
	_ scannerValuer = (*sqlTime)(nil)
	_ scannerValuer = (*sqlUploadID)(nil)
	_ scannerValuer = (*sqlMetaJSON)(nil)
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
		*t = sqlTime(time.Unix(src, 0))
		return nil
	default:
		return fmt.Errorf("cannot scan %T to time.Time", src)
	}
}

func (t sqlTime) Value() (driver.Value, error) {
	return time.Time(t).Unix(), nil
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

type sqlUploadID s3.UploadID

func (uid *sqlUploadID) Scan(src any) error {
	switch src := src.(type) {
	case []byte:
		if len(src) != len(s3.UploadID{}) {
			return fmt.Errorf("failed to scan source into UploadID due to invalid number of bytes %v != %v: %v", len(src), len(s3.UploadID{}), src)
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

type sqlMetaJSON map[string]string

func (m *sqlMetaJSON) Scan(src any) error {
	switch src := src.(type) {
	case string:
		return json.Unmarshal([]byte(src), m)
	case []byte:
		return json.Unmarshal(src, m)
	default:
		return fmt.Errorf("cannot scan %T to MetaJSON", src)
	}
}

func (m sqlMetaJSON) Value() (driver.Value, error) {
	data, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}
	return string(data), nil
}
