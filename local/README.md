monsterMQ Docker deployment (init provisioning)
---------------------------------------------

What this provides
- docker-compose stack (Postgres + monsterMQ + one-shot provisioner)
- mountable config.yaml (editable in repo root; mounted into container)
- init scripts to provision a sample admin user and a permissive ACL via GraphQL (idempotent attempts)
- example .env file

Files of interest
- docker-setup/docker-compose.yml
- docker-setup/.env.example
- docker-setup/init/provision.sh
- docker-setup/init/wait-for-postgres.sh
- ../config.yaml (mounted into the container at /etc/monstermq/config.yaml)

Quickstart (local, non-production)
1. Copy the example env and edit credentials:
   cp docker-setup/.env.example docker-setup/.env
   Edit docker-setup/.env and change POSTGRES_PASSWORD and ADMIN_PASS.

2. Make init scripts executable (required on Linux/macOS):
   chmod +x docker-setup/init/*.sh

3. Start the stack:
   docker compose -f docker-setup/docker-compose.yml up -d

4. Watch the provisioner logs (it runs once and then exits):
   docker compose -f docker-setup/docker-compose.yml logs -f provisioner

5. Verify:
   - Postgres volume is created (pgdata).
   - monsterMQ GraphQL UI/API should be reachable on localhost:4000 (or the host you mapped).
   - The provisioner will attempt to create user 'admin' with the password from ADMIN_PASS and add a permissive ACL. Inspect provisioner logs for GraphQL responses and adjust if the broker's GraphQL schema differs.

Notes and next steps
- The provisioning script uses plausible GraphQL mutation names. If your monsterMQ GraphQL schema uses different names/inputs, update docker-setup/init/provision.sh accordingly. The script prints API responses to help adapt it.
- Bridges are not automatically provisioned by default (per your request). Configure bridges manually via the broker UI/API or add JSON seeds and extend provision.sh if you want them automated.
- Do NOT keep production passwords in source control. Use Docker secrets, Kubernetes secrets, or an external secret manager for POSTGRES_PASSWORD and ADMIN_PASS.
- TLS: you requested external TLS termination. Put a reverse proxy (nginx/Caddy/Traefik) in front of the broker for HTTPS/ACME and to selectively expose ports.
- Persistent data: Postgres data is stored in a named volume 'pgdata' (declared in docker-compose). Ensure you have backups and retention as needed.

Note about .acl and editor warnings
- The file docker-setup/init/.acl is a plain-text ACL specification (not YAML). Some editors or workspace settings may attempt to parse files named *.acl as YAML and show "YAML Error" diagnostics — those diagnostics can be safely ignored for this file.
- If Visual Studio Code is flagging .acl as YAML and you prefer to remove the warnings, you can:
  - Change the file association in VS Code for the .acl extension, or
  - Rename the file to something like acl.txt and update docker-setup/init/provision.sh (ACL_FILE) accordingly.
- The provisioner reads the file at /docker-entrypoint-init/.acl inside the container; if you rename it, ensure docker-compose mounts it into that path or set ACL_FILE to the new path.

</final_file_content>
<task_progress>
- [x] Inspect project configuration (config.yaml)
- [x] Create docker-compose and provisioning scaffold
- [x] Add wait-for-postgres helper script
- [x] Add provisioning script and seed files (placeholder seeds added)
- [x] Add .env.example
- [x] Add README and usage instructions
- [x] Update provisioning script to support multiple users and .acl application
- [x] Mount .acl into init so provisioner can read it
- [ ] Verify provisioner applied users and ACLs (awaiting logs)
</task_progress>

Production hardening checklist (recommended)
- Replace example admin password immediately and avoid simple defaults.
- Use Docker secrets or env injection from your orchestrator rather than committing .env.
- Run the broker behind an authenticated reverse-proxy for management endpoints if needed.
- Configure monitoring/alerts and log shipping for broker logs and Postgres.
- Review ACL rules and test DisconnectOnUnauthorized behavior.

If you want, I will:
- Run chmod +x on the init scripts in this workspace so they're executable.
- Add a small example SQL or GraphQL seed file for a second user and a more restrictive ACL.
- Create a minimal reverse-proxy example (Caddy or Traefik) for TLS termination.
