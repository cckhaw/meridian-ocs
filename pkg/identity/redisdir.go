package identity

import (
	"context"
	"errors"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// RedisDirectory stores bindings as plain keys "<prefix><type>:<value>" whose
// value is the subscriber key, e.g.
//
//	SET id:msisdn:491701234567 acct-8812
//	SET id:imsi:262011234567890 acct-8812
//
// Each lookup is one GET and may live in any cluster slot; it is deliberately
// outside the subscriber's atomic charge script.
type RedisDirectory struct {
	rdb    redis.UniversalClient
	prefix string
	bind   *redis.Script
	unbind *redis.Script
}

// bindLua binds ARGV[1] to KEYS[1] unless it is bound to a different key
// (then only if ARGV[2] == "1"). Returns 1 newly bound, 3 already bound to the
// same key, 2 rebound from another key, 0 conflict.
const bindLua = `
local cur = redis.call('GET', KEYS[1])
if cur == false then redis.call('SET', KEYS[1], ARGV[1]) return 1 end
if cur == ARGV[1] then return 3 end
if ARGV[2] == '1' then redis.call('SET', KEYS[1], ARGV[1]) return 2 end
return 0`

// unbindIfLua deletes KEYS[1] only if it is still bound to ARGV[1].
const unbindIfLua = `
if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) end
return 0`

// NewRedisDirectory builds a directory. prefix defaults to "id:".
func NewRedisDirectory(rdb redis.UniversalClient, prefix string) *RedisDirectory {
	if prefix == "" {
		prefix = "id:"
	}
	return &RedisDirectory{rdb: rdb, prefix: prefix, bind: redis.NewScript(bindLua), unbind: redis.NewScript(unbindIfLua)}
}

func (d *RedisDirectory) redisKey(id Identity) (string, error) {
	v, ok := Normalize(id.Type, id.Value)
	if !ok {
		return "", ErrInvalid
	}
	return d.prefix + id.Type.String() + ":" + v, nil
}

// Lookup implements Directory.
func (d *RedisDirectory) Lookup(ctx context.Context, id Identity) (string, error) {
	k, err := d.redisKey(id)
	if err != nil {
		return "", err
	}
	v, err := d.rdb.Get(ctx, k).Result()
	switch {
	case errors.Is(err, redis.Nil):
		return "", ErrUnknown
	case err != nil:
		return "", err
	}
	return v, nil
}

// Bind attaches an identity to a subscriber key. It is idempotent for the same
// key and fails with ErrConflict if the identity belongs to another subscriber.
func (d *RedisDirectory) Bind(ctx context.Context, id Identity, key string) error {
	_, err := d.BindNew(ctx, id, key)
	return err
}

// BindNew is Bind that also reports whether the binding was created by this
// call (false: it already existed for the same key). Callers that bind several
// identities as one step use it to undo exactly what they added.
func (d *RedisDirectory) BindNew(ctx context.Context, id Identity, key string) (created bool, err error) {
	n, err := d.doBind(ctx, id, key, false)
	return n == 1, err
}

// Rebind moves an identity to another subscriber (SIM swap, number
// reassignment) and reports whether it was previously bound elsewhere. Callers
// holding a CachedDirectory see the change after at most its TTL.
func (d *RedisDirectory) Rebind(ctx context.Context, id Identity, key string) (moved bool, err error) {
	n, err := d.doBind(ctx, id, key, true)
	return n == 2, err
}

func (d *RedisDirectory) doBind(ctx context.Context, id Identity, key string, force bool) (int, error) {
	if !ValidKey(key) {
		return 0, fmt.Errorf("%w: subscriber key", ErrInvalid)
	}
	k, err := d.redisKey(id)
	if err != nil {
		return 0, err
	}
	f := "0"
	if force {
		f = "1"
	}
	n, err := d.bind.Run(ctx, d.rdb, []string{k}, key, f).Int()
	if err != nil {
		return 0, err
	}
	if n == 0 {
		return 0, ErrConflict
	}
	return n, nil
}

// UnbindIf removes an identity only if it is still bound to key, and reports
// whether it did. It never removes another subscriber's binding.
func (d *RedisDirectory) UnbindIf(ctx context.Context, id Identity, key string) (bool, error) {
	k, err := d.redisKey(id)
	if err != nil {
		return false, err
	}
	n, err := d.unbind.Run(ctx, d.rdb, []string{k}, key).Int()
	return n == 1, err
}

// Unbind removes an identity.
func (d *RedisDirectory) Unbind(ctx context.Context, id Identity) error {
	k, err := d.redisKey(id)
	if err != nil {
		return err
	}
	return d.rdb.Del(ctx, k).Err()
}
