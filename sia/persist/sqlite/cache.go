package sqlite

import (
	"sync"

	"github.com/SiaFoundation/s3d/s3"
)

type (
	// lookupCache holds the access key, bucket and lifecycle rows every request
	// reads before it reaches an object. Any write to those tables resets it.
	lookupCache struct {
		mu        sync.RWMutex
		gen       uint64
		secrets   map[string]cachedSecret
		buckets   map[string]cachedBucket
		lifecycle map[s3.BucketID]cachedLifecycle
	}

	cachedBucket struct {
		id     s3.BucketID
		owner  string
		policy string
	}

	cachedLifecycle struct {
		config  string
		missing bool
	}

	cachedSecret struct {
		secret   string
		userName string
	}
)

func (c *lookupCache) bucket(name string) (v cachedBucket, gen uint64, ok bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	v, ok = c.buckets[name]
	return v, c.gen, ok
}

func (c *lookupCache) lifecycleConfig(bucket s3.BucketID) (v cachedLifecycle, gen uint64, ok bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	v, ok = c.lifecycle[bucket]
	return v, c.gen, ok
}

func (c *lookupCache) putBucket(gen uint64, name string, v cachedBucket) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if gen != c.gen {
		return
	}
	if c.buckets == nil {
		c.buckets = make(map[string]cachedBucket)
	}
	c.buckets[name] = v
}

func (c *lookupCache) putLifecycleConfig(gen uint64, bucket s3.BucketID, v cachedLifecycle) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if gen != c.gen {
		return
	}
	if c.lifecycle == nil {
		c.lifecycle = make(map[s3.BucketID]cachedLifecycle)
	}
	c.lifecycle[bucket] = v
}

func (c *lookupCache) putSecret(gen uint64, accessKeyID string, v cachedSecret) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if gen != c.gen {
		return
	}
	if c.secrets == nil {
		c.secrets = make(map[string]cachedSecret)
	}
	c.secrets[accessKeyID] = v
}

func (c *lookupCache) reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.gen++
	clear(c.secrets)
	clear(c.buckets)
	clear(c.lifecycle)
}

func (c *lookupCache) secret(accessKeyID string) (v cachedSecret, gen uint64, ok bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	v, ok = c.secrets[accessKeyID]
	return v, c.gen, ok
}
