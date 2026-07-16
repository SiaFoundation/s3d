package sqlite

import (
	"fmt"

	"go.sia.tech/core/types"
	"go.sia.tech/indexd/slabs"
	sdk "go.sia.tech/siastorage"
)

// upsertSiaObject writes the sealed object to the normalized sia object
// tables, keyed by the id computed from its slab slices. Existing rows are
// refreshed: since the id is a digest of the object's slab slices, a matching
// id implies identical slices and slabs, so only the object's scalar fields
// and the sectors' host keys (which the digests do not cover) can actually
// change. Slabs shared with other objects are reused and keep their version.
func upsertSiaObject(tx *txn, sealed sdk.SealedObject) error {
	id := sealed.ID()

	var exists bool
	if err := tx.QueryRow(`SELECT EXISTS(SELECT 1 FROM sia_objects WHERE id = $1)`, sqlHash256(id)).Scan(&exists); err != nil {
		return fmt.Errorf("failed to check for sia object: %w", err)
	}

	if _, err := tx.Exec(`
		INSERT INTO sia_objects (id, encrypted_data_key, data_signature, encrypted_metadata_key, encrypted_metadata, metadata_signature, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		ON CONFLICT (id) DO UPDATE SET
			encrypted_data_key = excluded.encrypted_data_key,
			data_signature = excluded.data_signature,
			encrypted_metadata_key = excluded.encrypted_metadata_key,
			encrypted_metadata = excluded.encrypted_metadata,
			metadata_signature = excluded.metadata_signature,
			created_at = excluded.created_at,
			updated_at = excluded.updated_at
	`, sqlHash256(id), sealed.EncryptedDataKey, sqlSignature(sealed.DataSignature),
		sealed.EncryptedMetadataKey, sealed.EncryptedMetadata, sqlSignature(sealed.MetadataSignature),
		sqlTime(sealed.CreatedAt), sqlTime(sealed.UpdatedAt)); err != nil {
		return fmt.Errorf("failed to upsert sia object: %w", err)
	}

	// if the object already existed, its slices and the slabs they reference
	// are guaranteed to exist as well since the id is a digest over them;
	// only the sector host keys can have changed
	var slabStmt, sliceStmt *stmt
	if !exists {
		var err error
		slabStmt, err = tx.Prepare(`
			INSERT INTO sia_slabs (id, encryption_key, min_shards, version)
			VALUES ($1, $2, $3, $4)
			ON CONFLICT (id) DO NOTHING`)
		if err != nil {
			return err
		}
		defer slabStmt.Close()

		sliceStmt, err = tx.Prepare(`
			INSERT INTO sia_slab_slices (sia_object_id, slice_index, slab_id, offset, length)
			VALUES ($1, $2, $3, $4, $5)`)
		if err != nil {
			return err
		}
		defer sliceStmt.Close()
	}

	// the slab id is a digest over the sector roots, so a conflicting row
	// already has the correct root; only the host key can change
	sectorStmt, err := tx.Prepare(`
		INSERT INTO sia_slab_sectors (slab_id, sector_index, root, host_key)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (slab_id, sector_index) DO UPDATE SET host_key = excluded.host_key`)
	if err != nil {
		return err
	}
	defer sectorStmt.Close()

	for i, ss := range sealed.Slabs {
		slabID := ss.Digest()
		if !exists {
			if _, err := slabStmt.Exec(sqlHash256(slabID), sqlHash256(ss.EncryptionKey), int64(ss.MinShards), int64(ss.Version)); err != nil {
				return fmt.Errorf("failed to insert slab %v: %w", slabID, err)
			}
			if _, err := sliceStmt.Exec(sqlHash256(id), i, sqlHash256(slabID), int64(ss.Offset), int64(ss.Length)); err != nil {
				return fmt.Errorf("failed to insert slab slice %d: %w", i, err)
			}
		}
		for j, sec := range ss.Sectors {
			if _, err := sectorStmt.Exec(sqlHash256(slabID), j, sqlHash256(sec.Root), sqlHash256(sec.HostKey)); err != nil {
				return fmt.Errorf("failed to insert sector %d of slab %v: %w", j, slabID, err)
			}
		}
	}
	return nil
}

// siaObject loads the sealed object with the given id from the normalized sia
// object tables. It returns sql.ErrNoRows if no such object exists.
func siaObject(tx *txn, id types.Hash256) (sdk.SealedObject, error) {
	var so slabs.SealedObject
	err := tx.QueryRow(`
		SELECT encrypted_data_key, data_signature, encrypted_metadata_key, encrypted_metadata, metadata_signature, created_at, updated_at
		FROM sia_objects
		WHERE id = $1
	`, sqlHash256(id)).Scan(&so.EncryptedDataKey, (*sqlSignature)(&so.DataSignature),
		&so.EncryptedMetadataKey, &so.EncryptedMetadata, (*sqlSignature)(&so.MetadataSignature),
		(*sqlTime)(&so.CreatedAt), (*sqlTime)(&so.UpdatedAt))
	if err != nil {
		return sdk.SealedObject{}, err
	}

	rows, err := tx.Query(`
		SELECT s.version, s.encryption_key, s.min_shards, sl.offset, sl.length
		FROM sia_slab_slices sl
		JOIN sia_slabs s ON s.id = sl.slab_id
		WHERE sl.sia_object_id = $1
		ORDER BY sl.slice_index
	`, sqlHash256(id))
	if err != nil {
		return sdk.SealedObject{}, fmt.Errorf("failed to query slab slices: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var ss slabs.SlabSlice
		if err := rows.Scan(&ss.Version, (*sqlHash256)(&ss.EncryptionKey), &ss.MinShards, &ss.Offset, &ss.Length); err != nil {
			return sdk.SealedObject{}, fmt.Errorf("failed to scan slab slice: %w", err)
		}
		so.Slabs = append(so.Slabs, ss)
	}
	if err := rows.Err(); err != nil {
		return sdk.SealedObject{}, err
	}
	rows.Close()

	// attach the sectors; slices sharing a slab each get their own copy
	rows, err = tx.Query(`
		SELECT sl.slice_index, sec.root, sec.host_key
		FROM sia_slab_slices sl
		JOIN sia_slab_sectors sec ON sec.slab_id = sl.slab_id
		WHERE sl.sia_object_id = $1
		ORDER BY sl.slice_index, sec.sector_index
	`, sqlHash256(id))
	if err != nil {
		return sdk.SealedObject{}, fmt.Errorf("failed to query sectors: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var idx int
		var sec slabs.PinnedSector
		if err := rows.Scan(&idx, (*sqlHash256)(&sec.Root), (*sqlHash256)(&sec.HostKey)); err != nil {
			return sdk.SealedObject{}, fmt.Errorf("failed to scan sector: %w", err)
		}
		if idx < 0 || idx >= len(so.Slabs) {
			return sdk.SealedObject{}, fmt.Errorf("sector references slice %d of sia object %v with %d slices", idx, id, len(so.Slabs))
		}
		so.Slabs[idx].Sectors = append(so.Slabs[idx].Sectors, sec)
	}
	if err := rows.Err(); err != nil {
		return sdk.SealedObject{}, err
	}
	return sdk.SealedObject{SealedObject: so}, nil
}

// deleteSiaObject removes the sealed object with the given id along with its
// slab slices. Slabs no longer referenced by any slice are removed together
// with their sectors; slabs shared with other objects are kept.
func deleteSiaObject(tx *txn, id types.Hash256) error {
	// remove slabs referenced only by this object; their slices (which all
	// belong to this object) and their sectors are removed by the cascades
	if _, err := tx.Exec(`
		DELETE FROM sia_slabs
		WHERE id IN (SELECT slab_id FROM sia_slab_slices WHERE sia_object_id = $1)
		AND NOT EXISTS (
			SELECT 1 FROM sia_slab_slices sl
			WHERE sl.slab_id = sia_slabs.id AND sl.sia_object_id <> $1
		)
	`, sqlHash256(id)); err != nil {
		return fmt.Errorf("failed to delete slabs: %w", err)
	}
	// remove the object; its remaining slices (of shared slabs) cascade
	if _, err := tx.Exec(`DELETE FROM sia_objects WHERE id = $1`, sqlHash256(id)); err != nil {
		return fmt.Errorf("failed to delete sia object: %w", err)
	}
	return nil
}
