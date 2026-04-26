package integration

import (
	"testing"

	"monstermq.io/edge/internal/config"
)

// TestLoginAcceptsAnyCredentialsWhenUserMgmtDisabled covers the dashboard's
// login flow when user management is off — any credentials must succeed.
func TestLoginAcceptsAnyCredentialsWhenUserMgmtDisabled(t *testing.T) {
	srv, url := startWithGraphQL(t, 23006, 28006, func(c *config.Config) {
		c.UserManagement.Enabled = false
	})
	defer srv.Close()

	data := gqlQuery(t, url, `mutation { login(username: "anyone", password: "anything") { success message username isAdmin token } }`, nil)
	login := data["login"].(map[string]any)
	// Match the JVM broker's anonymous response shape exactly.
	if !login["success"].(bool) {
		t.Fatalf("login should succeed, got %v", login)
	}
	if login["message"] != "Authentication disabled" {
		t.Fatalf("expected message=Authentication disabled, got %v", login["message"])
	}
	if login["username"] != "anonymous" {
		t.Fatalf("expected username=anonymous, got %v", login["username"])
	}
	if login["isAdmin"] != true {
		t.Fatalf("expected isAdmin=true, got %v", login["isAdmin"])
	}
	if login["token"] != nil {
		t.Fatalf("expected token=null, got %v", login["token"])
	}
}

// TestLoginAcceptsAnyCredentialsWhenAnonymous: even with user mgmt enabled,
// anonymous mode means any login succeeds.
func TestLoginAcceptsAnyCredentialsWhenAnonymous(t *testing.T) {
	srv, url := startWithGraphQL(t, 23007, 28007, func(c *config.Config) {
		c.UserManagement.Enabled = true
		c.UserManagement.AnonymousEnabled = true
	})
	defer srv.Close()

	data := gqlQuery(t, url, `mutation { login(username: "x", password: "y") { success } }`, nil)
	if !data["login"].(map[string]any)["success"].(bool) {
		t.Fatal("anonymous mode should accept login")
	}
}
