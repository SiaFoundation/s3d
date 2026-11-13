package sqlite

import "testing"

func TestLookupCacheReset(t *testing.T) {
	var c lookupCache

	// a reset between the miss and the insert drops the insert
	_, gen, ok := c.secret("key")
	if ok {
		t.Fatal("unexpected hit")
	}
	c.reset()
	c.putSecret(gen, "key", cachedSecret{secret: "stale"})
	if _, _, ok := c.secret("key"); ok {
		t.Fatal("stale secret cached")
	}
	_, gen, _ = c.secret("key")
	c.putSecret(gen, "key", cachedSecret{secret: "fresh"})
	if v, _, ok := c.secret("key"); !ok || v.secret != "fresh" {
		t.Fatal("unexpected", v, ok)
	}

	_, gen, _ = c.bucket("bucket")
	c.reset()
	c.putBucket(gen, "bucket", cachedBucket{id: 1})
	if _, _, ok := c.bucket("bucket"); ok {
		t.Fatal("stale bucket cached")
	}
	_, gen, _ = c.bucket("bucket")
	c.putBucket(gen, "bucket", cachedBucket{id: 1})
	if v, _, ok := c.bucket("bucket"); !ok || v.id != 1 {
		t.Fatal("unexpected", v, ok)
	}

	_, gen, _ = c.lifecycleConfig(1)
	c.reset()
	c.putLifecycleConfig(gen, 1, cachedLifecycle{missing: true})
	if _, _, ok := c.lifecycleConfig(1); ok {
		t.Fatal("stale lifecycle cached")
	}
	_, gen, _ = c.lifecycleConfig(1)
	c.putLifecycleConfig(gen, 1, cachedLifecycle{missing: true})
	if v, _, ok := c.lifecycleConfig(1); !ok || !v.missing {
		t.Fatal("unexpected", v, ok)
	}

	// a reset clears every entry
	c.reset()
	if _, _, ok := c.secret("key"); ok {
		t.Fatal("secret survived the reset")
	} else if _, _, ok := c.bucket("bucket"); ok {
		t.Fatal("bucket survived the reset")
	} else if _, _, ok := c.lifecycleConfig(1); ok {
		t.Fatal("lifecycle survived the reset")
	}
}
