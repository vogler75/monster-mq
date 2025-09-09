#!/bin/bash

# Test script for JWT-based GraphQL authorization

GRAPHQL_URL="http://localhost:4000/graphql"

echo "Testing JWT-based GraphQL Authorization..."

# Test 1: Login with default admin user
echo "1. Testing login with admin user..."
LOGIN_RESULT=$(curl -s -X POST \
  -H "Content-Type: application/json" \
  -d '{
    "query": "mutation { login(input: { username: \"admin\", password: \"admin\" }) { success token message username isAdmin } }"
  }' \
  $GRAPHQL_URL)

echo "Login result: $LOGIN_RESULT"

# Extract token from login result using jq if available, otherwise use grep
if command -v jq &> /dev/null; then
    TOKEN=$(echo "$LOGIN_RESULT" | jq -r '.data.login.token // empty')
else
    TOKEN=$(echo "$LOGIN_RESULT" | grep -o '"token":"[^"]*"' | cut -d'"' -f4)
fi

if [ -z "$TOKEN" ]; then
    echo "❌ Login failed or token not found"
    exit 1
else
    echo "✅ Login successful, token: ${TOKEN:0:20}..."
fi

# Test 2: Try to access protected query without token
echo "2. Testing access without token (should fail)..."
NO_TOKEN_RESULT=$(curl -s -X POST \
  -H "Content-Type: application/json" \
  -d '{
    "query": "query { searchTopics(pattern: \"test\") }"
  }' \
  $GRAPHQL_URL)

echo "No token result: $NO_TOKEN_RESULT"

# Test 3: Try to access protected query with token
echo "3. Testing access with token (should succeed)..."
WITH_TOKEN_RESULT=$(curl -s -X POST \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "query": "query { searchTopics(pattern: \"test\") }"
  }' \
  $GRAPHQL_URL)

echo "With token result: $WITH_TOKEN_RESULT"

# Test 4: Try to access admin-only mutation with admin token
echo "4. Testing admin operation with admin token (should succeed)..."
ADMIN_RESULT=$(curl -s -X POST \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "query": "query { getAllUsers { username enabled isAdmin } }"
  }' \
  $GRAPHQL_URL)

echo "Admin operation result: $ADMIN_RESULT"

echo "Test completed!"