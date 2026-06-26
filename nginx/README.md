# Nginx

Reverse proxy that exposes Kafka UI on port 80 and isolates the
backend on `localhost:8080`. Includes selective access logging
(only API and authentication calls) and 3-day log retention.

## Folder structure

```
nginx/
├── README.md
├── kafka-ui.conf             # Site config (sites-available)
└── kafka-ui-logrotate        # /etc/logrotate.d/kafka-ui
```

## Prerequisites

- Nginx 1.18+ installed (`sudo apt install -y nginx`)
- Kafka UI container running and bound to `localhost:8080`
  (see [`../kafka-ui/`](../kafka-ui/))

## Installation

```bash
# Copy the site config
sudo cp nginx/kafka-ui.conf /etc/nginx/sites-available/kafka-ui

# Remove the default site
sudo rm /etc/nginx/sites-enabled/default

# Enable our site
sudo ln -s /etc/nginx/sites-available/kafka-ui /etc/nginx/sites-enabled/

# Install logrotate policy
sudo cp nginx/kafka-ui-logrotate /etc/logrotate.d/kafka-ui

# Test and apply
sudo nginx -t && sudo systemctl restart nginx
```

## Why a reverse proxy?

- Hides the internal `:8080` port from users — they hit `http://host/`.
- Allows selective access logging (assets produce noise; API calls are
  what matters for audit) [1].
- Centralises TLS termination when a certificate is added later.

## Configuration

The config file uses **`server_name _;`** as a catch-all so it works
on any host without modification. Replace it with your actual
hostname or leave it as-is for internal-only access [1].

### `kafka-ui.conf`

```nginx
log_format kafka_ui '$time_local | $remote_addr | $request | $status';

server {
    listen 80;
    server_name _;

    error_log /var/log/nginx/kafka-ui-error.log;

    # Static assets — no log (high volume, low value)
    location ~* \.(js|css|svg|ico|ttf|woff|woff2|png|jpg|json)$ {
        proxy_pass http://localhost:8080;
        access_log off;
    }

    # API and authentication — with log (audit trail)
    location ~* ^/(api|auth) {
        proxy_pass http://localhost:8080;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_http_version 1.1;
        access_log /var/log/nginx/kafka-ui-access.log kafka_ui;
    }

    # Everything else — no log
    location / {
        proxy_pass http://localhost:8080;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
        access_log off;
    }
}
```

## Access logs

Only API and auth calls are recorded. Static asset requests are
intentionally silenced to keep logs small and relevant [1].

```bash
# Monitor in real time
sudo tail -f /var/log/nginx/kafka-ui-access.log

# Errors only
sudo tail -f /var/log/nginx/kafka-ui-error.log
```

Log format:
```
23/Jun/2026:10:44:25 -0300 | 10.10.0.124 | GET /api/clusters HTTP/1.1 | 200
23/Jun/2026:10:44:25 -0300 | 10.10.0.124 | POST /auth HTTP/1.1 | 302
```

## Log retention

A `logrotate` policy keeps the last 3 days of access logs and
reopens the file daily without restarting Nginx [1].

### `kafka-ui-logrotate`

```
/var/log/nginx/kafka-ui-access.log {
    daily
    rotate 3
    compress
    missingok
    notifempty
    sharedscripts
    postrotate
        nginx -s reopen
    endscript
}
```

Test:
```bash
sudo logrotate -d /etc/logrotate.d/kafka-ui
```

## Timezone

Logs use the server's local timezone. For consistent timestamps in
GMT-3 (America/Sao_Paulo):

```bash
sudo timedatectl set-timezone America/Sao_Paulo
timedatectl
```

## Useful commands

```bash
# Status
sudo systemctl status nginx

# Test config without restart
sudo nginx -t

# Reload (zero-downtime)
sudo systemctl reload nginx

# Full restart
sudo systemctl restart nginx

# Enable at boot
sudo systemctl enable nginx
```
