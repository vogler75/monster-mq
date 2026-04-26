package auth

import (
	"context"
	"sort"
	"strings"
	"sync"
	"time"

	"monstermq.io/edge/internal/stores"
)

// Cache holds users and ACL rules in memory and refreshes from the UserStore.
type Cache struct {
	store        stores.UserStore
	mu           sync.RWMutex
	users        map[string]stores.User
	rulesByUser  map[string][]stores.AclRule
	anonymousAllow bool
}

func NewCache(store stores.UserStore, anonymousAllow bool) *Cache {
	return &Cache{
		store:          store,
		users:          map[string]stores.User{},
		rulesByUser:    map[string][]stores.AclRule{},
		anonymousAllow: anonymousAllow,
	}
}

func (c *Cache) Refresh(ctx context.Context) error {
	users, rules, err := c.store.LoadAll(ctx)
	if err != nil {
		return err
	}
	rulesByUser := map[string][]stores.AclRule{}
	for _, r := range rules {
		rulesByUser[r.Username] = append(rulesByUser[r.Username], r)
	}
	for u, list := range rulesByUser {
		sort.Slice(list, func(i, j int) bool { return list[i].Priority > list[j].Priority })
		rulesByUser[u] = list
	}
	usersMap := map[string]stores.User{}
	for _, u := range users {
		usersMap[u.Username] = u
	}
	c.mu.Lock()
	c.users = usersMap
	c.rulesByUser = rulesByUser
	c.mu.Unlock()
	return nil
}

// StartRefresher periodically reloads users/ACL from the underlying store.
func (c *Cache) StartRefresher(ctx context.Context, every time.Duration) {
	go func() {
		t := time.NewTicker(every)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				_ = c.Refresh(ctx)
			}
		}
	}()
}

// Validate returns true if (username, password) match an enabled user.
func (c *Cache) Validate(username, password string) bool {
	if username == "" {
		return c.anonymousAllow
	}
	u, ok := c.lookup(username)
	if !ok || !u.Enabled {
		return false
	}
	// Bcrypt verification goes through the store to avoid duplicating the cost.
	res, err := c.store.ValidateCredentials(context.Background(), username, password)
	return err == nil && res != nil
}

func (c *Cache) lookup(username string) (stores.User, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	u, ok := c.users[username]
	return u, ok
}

// Allow returns true if the user is permitted to publish (write=true) or subscribe
// (write=false) to topic.
func (c *Cache) Allow(username, topic string, write bool) bool {
	if username == "" {
		return c.anonymousAllow
	}
	u, ok := c.lookup(username)
	if !ok || !u.Enabled {
		return false
	}
	if u.IsAdmin {
		return true
	}
	if write && !u.CanPublish {
		return false
	}
	if !write && !u.CanSubscribe {
		return false
	}
	c.mu.RLock()
	rules := c.rulesByUser[username]
	c.mu.RUnlock()
	if len(rules) == 0 {
		return true
	}
	for _, r := range rules {
		if topicMatches(r.TopicPattern, topic) {
			if write {
				return r.CanPublish
			}
			return r.CanSubscribe
		}
	}
	return false
}

func topicMatches(pattern, topic string) bool {
	pp := strings.Split(pattern, "/")
	tt := strings.Split(topic, "/")
	for i, p := range pp {
		if p == "#" {
			return true
		}
		if i >= len(tt) {
			return false
		}
		if p == "+" {
			continue
		}
		if p != tt[i] {
			return false
		}
	}
	return len(pp) == len(tt)
}
