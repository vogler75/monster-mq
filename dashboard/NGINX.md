# Standalone nginx Deployment for MonsterMQ Dashboard

This guide covers deploying the dashboard as a standalone static site behind nginx, with reverse proxy to the MonsterMQ broker for GraphQL API access.

## Build the Dashboard

```bash
cd dashboard
npm ci
npm run build
```

The built files will be in `dashboard/dist/`.

## nginx Configuration

```nginx
server {
    listen 80;
    server_name monstermq.example.com;

    # Serve dashboard static files
    root /path/to/dashboard/dist;
    index pages/login.html;

    location / {
        try_files $uri $uri/ /pages/login.html;
    }

    # Reverse proxy for GraphQL API
    location /graphql {
        proxy_pass http://localhost:4000/graphql;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }

    # WebSocket proxy for GraphQL subscriptions
    location /graphqlws {
        proxy_pass http://localhost:4000/graphqlws;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_read_timeout 86400;
    }
}
```

## Docker Example

```dockerfile
FROM node:20-alpine AS build
WORKDIR /app
COPY dashboard/package*.json ./
RUN npm ci
COPY dashboard/ ./
RUN npm run build

FROM nginx:alpine
COPY --from=build /app/dist /usr/share/nginx/html
COPY nginx.conf /etc/nginx/conf.d/default.conf
EXPOSE 80
```

Where `nginx.conf` uses the configuration above with `root /usr/share/nginx/html;`.
