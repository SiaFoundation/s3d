package sqlite

import (
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/indexd/slabs"
)

// This file contains frozen copies of the sealed object types from
// go.sia.tech/indexd/slabs as they were encoded before slab versioning was
// introduced (indexd v0.3.2). Migrations use them to decode sia objects
// stored with the legacy encoding. They must not be updated when the SDK
// types change; a new encoding change requires a new set of legacy types and
// a new migration.
type (
	legacyPinnedSector struct {
		Root    types.Hash256
		HostKey types.PublicKey
	}

	legacySlabSlice struct {
		EncryptionKey [32]byte
		MinShards     uint
		Sectors       []legacyPinnedSector
		Offset        uint32
		Length        uint32
	}

	legacySealedObject struct {
		EncryptedDataKey     []byte
		Slabs                []legacySlabSlice
		DataSignature        types.Signature
		EncryptedMetadataKey []byte
		EncryptedMetadata    []byte
		MetadataSignature    types.Signature
		CreatedAt            time.Time
		UpdatedAt            time.Time
	}
)

// EncodeTo implements types.EncoderTo.
func (ps legacyPinnedSector) EncodeTo(e *types.Encoder) {
	ps.Root.EncodeTo(e)
	ps.HostKey.EncodeTo(e)
}

// DecodeFrom implements types.DecoderFrom.
func (ps *legacyPinnedSector) DecodeFrom(d *types.Decoder) {
	ps.Root.DecodeFrom(d)
	ps.HostKey.DecodeFrom(d)
}

// EncodeTo implements types.EncoderTo.
func (s legacySlabSlice) EncodeTo(e *types.Encoder) {
	e.Write(s.EncryptionKey[:])
	e.WriteUint8(uint8(s.MinShards))
	types.EncodeSlice(e, s.Sectors)
	e.WriteUint64(uint64(s.Offset)<<32 | uint64(s.Length))
}

// DecodeFrom implements types.DecoderFrom.
func (s *legacySlabSlice) DecodeFrom(d *types.Decoder) {
	d.Read(s.EncryptionKey[:])
	s.MinShards = uint(d.ReadUint8())
	types.DecodeSlice(d, &s.Sectors)
	combined := d.ReadUint64()
	s.Offset = uint32(combined >> 32)
	s.Length = uint32(combined)
}

// EncodeTo implements types.EncoderTo.
func (so legacySealedObject) EncodeTo(e *types.Encoder) {
	e.WriteBytes(so.EncryptedDataKey)
	types.EncodeSlice(e, so.Slabs)
	so.DataSignature.EncodeTo(e)
	e.WriteBytes(so.EncryptedMetadataKey)
	e.WriteBytes(so.EncryptedMetadata)
	so.MetadataSignature.EncodeTo(e)
	e.WriteTime(so.CreatedAt)
	e.WriteTime(so.UpdatedAt)
}

// DecodeFrom implements types.DecoderFrom.
func (so *legacySealedObject) DecodeFrom(d *types.Decoder) {
	so.EncryptedDataKey = d.ReadBytes()
	types.DecodeSlice(d, &so.Slabs)
	so.DataSignature.DecodeFrom(d)
	so.EncryptedMetadataKey = d.ReadBytes()
	so.EncryptedMetadata = d.ReadBytes()
	so.MetadataSignature.DecodeFrom(d)
	so.CreatedAt = d.ReadTime()
	so.UpdatedAt = d.ReadTime()
}

// convert converts the legacy sealed object to the current SDK type. Slabs
// uploaded before versioning was introduced are version 0, which keeps their
// digests and thereby the object's ID unchanged.
func (so legacySealedObject) convert() slabs.SealedObject {
	converted := slabs.SealedObject{
		EncryptedDataKey:     so.EncryptedDataKey,
		DataSignature:        so.DataSignature,
		EncryptedMetadataKey: so.EncryptedMetadataKey,
		EncryptedMetadata:    so.EncryptedMetadata,
		MetadataSignature:    so.MetadataSignature,
		CreatedAt:            so.CreatedAt,
		UpdatedAt:            so.UpdatedAt,
	}
	for _, ss := range so.Slabs {
		converted.Slabs = append(converted.Slabs, slabs.SlabSlice{
			Version:       0,
			EncryptionKey: ss.EncryptionKey,
			MinShards:     ss.MinShards,
			Sectors:       convertLegacySectors(ss.Sectors),
			Offset:        ss.Offset,
			Length:        ss.Length,
		})
	}
	return converted
}

func convertLegacySectors(sectors []legacyPinnedSector) []slabs.PinnedSector {
	converted := make([]slabs.PinnedSector, 0, len(sectors))
	for _, sec := range sectors {
		converted = append(converted, slabs.PinnedSector{
			Root:    sec.Root,
			HostKey: sec.HostKey,
		})
	}
	return converted
}
