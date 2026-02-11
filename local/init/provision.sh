#!/bin/sh
# provision.sh
# Idempotent provisioning script to:
#  - create users discovered in an .acl file (or the single ADMIN_USER if .acl absent)
#  - apply ACL entries from an .acl file via the broker's GraphQL API
#
# Behavior:
#  - If /docker-entrypoint-init/.acl exists it will be parsed. Blocks start with 'user <name>'
#    followed by 'topic ...' or 'pattern ...' lines which will be applied for that user.
#  - For each discovered user we create a user if it doesn't exist. Password for each user is:
#      <username>_<USER_PASS_SUFFIX>
#    where USER_PASS_SUFFIX defaults to ChangeMe123 (override with env var USER_PASS_SUFFIX).
#  - For each topic/pattern line we attempt a GraphQL createAcl mutation using principal "user:<name>"
#    and topic set to the provided topic/pattern. The mutation used is the same plausible mutation
#    used previously; if your GraphQL schema differs, update the mutation payloads below.
#
# Env vars:
#  - GRAPHQL_URL (default: http://monstermq:4000/graphql)
#  - WAIT_TIMEOUT (seconds, default: 120)
#  - ADMIN_USER / ADMIN_PASS (used if .acl absent; ADMIN_PASS also used as suffix fallback)
#  - USER_PASS_SUFFIX (default: ChangeMe123) -- suffix appended to username for generated passwords
#  - ACL_FILE (default: /docker-entrypoint-init/.acl)
#
# Notes:
#  - The script prints GraphQL responses for troubleshooting. Adapt the createUser/createAcl payloads
#    if your monsterMQ GraphQL schema uses different mutation names/inputs.
#  - For production, supply explicit passwords or provision users via a safer workflow.

set -eu

GRAPHQL_URL=${GRAPHQL_URL:-http://monstermq:4000/graphql}
WAIT_TIMEOUT=${WAIT_TIMEOUT:-120}
ACL_FILE=${ACL_FILE:-/docker-entrypoint-init/.acl}
ADMIN_USER=${ADMIN_USER:-Admin}
ADMIN_PASS=${ADMIN_PASS:-Admin}
USER_PASS_SUFFIX=${USER_PASS_SUFFIX:-ChangeMe123}

echo "Provisioning: GraphQL=${GRAPHQL_URL}, ACL_FILE=${ACL_FILE}"

# Wait for GraphQL to respond
start_ts=$(date +%s)
while true; do
  code=$(curl -s -o /dev/null -w "%{http_code}" "${GRAPHQL_URL}" || true)
  if [ "$code" = "200" ] || [ "$code" = "400" ]; then
    echo "GraphQL endpoint reachable (HTTP ${code})."
    break
  fi
  now=$(date +%s)
  elapsed=$((now - start_ts))
  if [ "$elapsed" -ge "$WAIT_TIMEOUT" ]; then
    echo "Timed out waiting for GraphQL endpoint at ${GRAPHQL_URL}" >&2
    exit 1
  fi
  sleep 2
done

graphql() {
  payload="$1"
  # Send GraphQL request; include Authorization header if TOKEN is set.
  if [ -n "${TOKEN:-}" ]; then
    curl -s -X POST "${GRAPHQL_URL}" \
      -H "Content-Type: application/json" \
      -H "Authorization: Bearer ${TOKEN}" \
      -d "{\"query\":${payload}}"
  else
    curl -s -X POST "${GRAPHQL_URL}" \
      -H "Content-Type: application/json" \
      -d "{\"query\":${payload}}"
  fi
}

# Attempt to authenticate as admin to obtain a JWT for admin-only mutations.
authenticate() {
  echo "Attempting login for ADMIN_USER=${ADMIN_USER}..."
  # Build the login payload properly escaping the variables
  login_json='{"query":"mutation Login($username: String!, $password: String!) { login(username: $username, password: $password) { success token isAdmin message username } }","variables":{"username":"'${ADMIN_USER}'","password":"'${ADMIN_PASS}'"}}'
  
  echo "Sending login request..."
  login_resp=$(curl -s -X POST "${GRAPHQL_URL}" -H "Content-Type: application/json" -d "${login_json}")
  echo "Login response: ${login_resp}"
  
  # Extract token using grep and sed for better compatibility
  TOKEN=$(echo "$login_resp" | grep -o '"token":"[^"]*"' | sed 's/"token":"//;s/"$//' || true)
  
  if [ -n "$TOKEN" ] && [ "$TOKEN" != "null" ]; then
    echo "Obtained admin token: ${TOKEN:0:20}..."
    export TOKEN
  else
    TOKEN=""
    echo "No admin token obtained; admin mutations may be unavailable."
    echo "Debug: Check if login mutation exists and credentials are correct."
  fi
}

# helper: create a user if missing
create_user_if_missing() {
  username="$1"
  password="$2"

  echo "Checking user '$username'..."
  resp=$(graphql '"query { users { username } }"')
  echo "Users query response: ${resp}"
  
  if echo "$resp" | grep -q "\"username\":\"${username}\""; then
    echo "User '${username}' already exists. Skipping creation."
    return 0
  fi

  echo "Creating user '${username}'..."
  # Use the correct mutation structure: user { createUser(...) { success message } }
  create_json='{"query":"mutation CreateUser($username: String!, $password: String!) { user { createUser(input: { username: $username, password: $password }) { success message } } }","variables":{"username":"'${username}'","password":"'${password}'"}}'
  if [ -n "${TOKEN:-}" ]; then
    create_resp=$(curl -s -X POST "${GRAPHQL_URL}" \
      -H "Content-Type: application/json" \
      -H "Authorization: Bearer ${TOKEN}" \
      -d "${create_json}")
  else
    create_resp=$(curl -s -X POST "${GRAPHQL_URL}" \
      -H "Content-Type: application/json" \
      -d "${create_json}")
  fi
  echo "Create user response: ${create_resp}"
  
  if echo "$create_resp" | grep -q '"success":true'; then
    echo "Created user '${username}'."
    return 0
  else
    echo "Failed to create user '${username}'. Response above."
    return 1
  fi
}

# helper: create an ACL entry
create_acl_entry() {
  principal="$1"  # e.g. user:edge-user
  topic="$2"      # topic or pattern string
  
  # Extract username from principal (format is "user:username")
  username=$(echo "$principal" | sed 's/^user://')
  
  echo "Creating ACL for username='${username}' topicPattern='${topic}'..."
  
  # Use the correct mutation structure with username and topicPattern
  acl_json='{"query":"mutation CreateAclRule($username: String!, $topicPattern: String!) { user { createAclRule(input: { username: $username, topicPattern: $topicPattern }) { success message } } }","variables":{"username":"'${username}'","topicPattern":"'${topic}'"}}'
  if [ -n "${TOKEN:-}" ]; then
    acl_resp=$(curl -s -X POST "${GRAPHQL_URL}" \
      -H "Content-Type: application/json" \
      -H "Authorization: Bearer ${TOKEN}" \
      -d "${acl_json}")
  else
    acl_resp=$(curl -s -X POST "${GRAPHQL_URL}" \
      -H "Content-Type: application/json" \
      -d "${acl_json}")
  fi
  echo "Create ACL response: ${acl_resp}"
}

# MAIN

# Try to authenticate before running mutations (graceful if login not supported)
echo "=== Starting authentication phase ==="
authenticate || {
  echo "Warning: Authentication failed or not supported. Continuing without token..."
  TOKEN=""
}

if [ -f "${ACL_FILE}" ]; then
  echo "ACL file found: ${ACL_FILE} — parsing..."

  current_user=""
  # Read file line by line
  while IFS= read -r rawline || [ -n "$rawline" ]; do
    # Trim leading/trailing whitespace
    line=$(echo "$rawline" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')
    # Skip comments and empty lines
    case "$line" in
      ''|\#*|';'*)
        continue
        ;;
    esac

    # Match "user <name>"
    if echo "$line" | grep -E -q '^user[[:space:]]+'; then
      current_user=$(echo "$line" | awk '{print $2}')
      echo "Found user block: ${current_user}"
      # build a generated password for the user (fixed to 123)
      genpass="123"
      create_user_if_missing "${current_user}" "${genpass}" || echo "Warning: failed to create user ${current_user}"
      continue
    fi

    # Match "topic <permission> <topic>"
    if echo "$line" | grep -E -q '^topic[[:space:]]+'; then
      # fields: topic <perm> <topicString...>
      # We only extract the topic string (2nd or 3rd field)
      # Example: topic readwrite edge/#
      topic_str=$(echo "$line" | awk '{for(i=3;i<=NF;i++){printf $i; if(i<NF) printf " "}}')
      # Some files may use format: topic read edge/#
      if [ -z "$topic_str" ]; then
        # fallback: take second field
        topic_str=$(echo "$line" | awk '{print $2}')
      fi
      if [ -n "${current_user}" ]; then
        create_acl_entry "user:${current_user}" "${topic_str}"
      else
        echo "No current user for topic line: ${line}"
      fi
      continue
    fi

    # Match "pattern <perm> <pattern>"
    if echo "$line" | grep -E -q '^pattern[[:space:]]+'; then
      pattern_str=$(echo "$line" | awk '{for(i=3;i<=NF;i++){printf $i; if(i<NF) printf " "}}')
      if [ -z "$pattern_str" ]; then
        pattern_str=$(echo "$line" | awk '{print $2}')
      fi
      if [ -n "${current_user}" ]; then
        create_acl_entry "user:${current_user}" "${pattern_str}"
      else
        echo "No current user for pattern line: ${line}"
      fi
      continue
    fi

    # If none matched, ignore but print for debugging
    echo "Unrecognized line (ignored): ${line}"
  done < "${ACL_FILE}"

  echo "Finished applying ACL file."
else
  echo "No ACL file found at ${ACL_FILE}. Falling back to single admin user provisioning."

  # Ensure single admin user exists
  genpass="123"
  create_user_if_missing "${ADMIN_USER}" "${genpass}" || echo "Warning: failed to create admin user ${ADMIN_USER}"

  # Create a permissive ACL for admin (as before)
  echo "Creating permissive ACL for admin user..."
  create_acl_entry "user:${ADMIN_USER}" "#"
fi

echo "Provisioning complete. Review the GraphQL responses above to confirm success."
