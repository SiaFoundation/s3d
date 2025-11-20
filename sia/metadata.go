package sia

import (
	"bytes"
	"fmt"
	"maps"
	"slices"
	"sort"

	"go.sia.tech/core/types"
)

type objectMeta struct {
	contentMD5 [16]byte
	meta       map[string]string
}

func (om *objectMeta) encode() ([]byte, error) {
	buf := new(bytes.Buffer)
	enc := types.NewEncoder(buf)
	_, _ = enc.Write(om.contentMD5[:])
	enc.WriteUint64(uint64(len(om.meta)))

	keys := slices.Collect(maps.Keys(om.meta))
	sort.Strings(keys)
	for _, k := range keys {
		enc.WriteString(k)
		enc.WriteString(om.meta[k])
	}
	if err := enc.Flush(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (om *objectMeta) decode(data []byte) error {
	dec := types.NewBufDecoder(data)
	n, err := dec.Read(om.contentMD5[:])
	if err != nil {
		return err
	} else if n != len(om.contentMD5) {
		return fmt.Errorf("invalid object metadata")
	}
	om.meta = make(map[string]string)
	nPairs := dec.ReadUint64()
	for range nPairs {
		k, v := dec.ReadString(), dec.ReadString()
		if dec.Err() != nil {
			return dec.Err()
		}
		om.meta[k] = v
	}
	return dec.Err()
}
