package sqlite

import (
	"database/sql"
	"database/sql/driver"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/SiaFoundation/s3d/s3"
	"go.sia.tech/core/types"
	sdk "go.sia.tech/siastorage"
)

var (
	_ scannerValuer = (*sqlHash256)(nil)
	_ scannerValuer = (*sqlMD5)(nil)
	_ scannerValuer = (*sqlTime)(nil)
	_ scannerValuer = (*sqlUploadID)(nil)
	_ scannerValuer = (*sqlSiaObject)(nil)
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

type sqlSiaObject sdk.SealedObject

func (m *sqlSiaObject) Scan(src any) error {
	switch src := src.(type) {
	case []byte:
		if len(src) == 0 {
			*m = sqlSiaObject{}
			return nil
		}
		return (*sdk.SealedObject)(m).UnmarshalSia(src)
	default:
		return fmt.Errorf("cannot scan %T to SiaObject", src)
	}
}

func (m sqlSiaObject) Value() (driver.Value, error) {
	so := sdk.SealedObject(m)
	return so.MarshalSia()
}

type sqlMetaJSON map[string]string

const metaHexPrefix = "hex:"

func (m *sqlMetaJSON) Scan(src any) error {
	var raw []byte
	switch src := src.(type) {
	case string:
		raw = []byte(src)
	case []byte:
		raw = src
	default:
		return fmt.Errorf("cannot scan %T to MetaJSON", src)
	}

	if err := json.Unmarshal(raw, m); err != nil {
		return err
	}

	for k, v := range *m {
		if after, ok := strings.CutPrefix(v, metaHexPrefix); ok {
			decoded, err := hex.DecodeString(after)
			if err != nil {
				return fmt.Errorf("failed to hex-decode metadata value for key %q: %w", k, err)
			}
			(*m)[k] = string(decoded)
		}
	}
	return nil
}

func (m sqlMetaJSON) Value() (driver.Value, error) {
	encoded := make(map[string]string, len(m))
	for k, v := range m {
		if !utf8.ValidString(v) || strings.HasPrefix(v, metaHexPrefix) {
			encoded[k] = metaHexPrefix + hex.EncodeToString([]byte(v))
		} else {
			encoded[k] = v
		}
	}

	data, err := json.Marshal(encoded)
	if err != nil {
		return nil, err
	}
	return string(data), nil
}
