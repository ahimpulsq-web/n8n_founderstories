# Deployment Guide - N8N-FounderStories

This guide covers production deployment options for the N8N-FounderStories application.

## 📋 Pre-Deployment Checklist

- [ ] PostgreSQL database is set up and accessible
- [ ] All required API keys are obtained
- [ ] Google Service Account is created with Sheets/Drive API access
- [ ] Service account JSON file is downloaded
- [ ] Environment variables are configured
- [ ] SSL certificates are ready (for HTTPS)
- [ ] Backup strategy is planned

## 🐳 Docker Deployment (Recommended)

### Option 1: Docker Compose (Easiest)

**Step 1: Prepare Environment**
```bash
# Clone repository
git clone <repository-url>
cd N8N-FounderStories

# Copy and configure environment
cp .env.example .env
nano .env  # Edit with your values

# Create credentials directory
mkdir -p credentials
# Place your service-account.json in credentials/
```

**Step 2: Start Services**
```bash
# Build and start all services
docker-compose up -d

# View logs
docker-compose logs -f app

# Check status
docker-compose ps
```

**Step 3: Verify Deployment**
```bash
# Health check
curl http://localhost:8000/api/v1/health

# View application logs
docker-compose logs -f app

# View database logs
docker-compose logs -f postgres
```

**Step 4: Manage Services**
```bash
# Stop services
docker-compose stop

# Restart services
docker-compose restart

# Stop and remove containers (keeps data)
docker-compose down

# Stop and remove everything including volumes (⚠️ deletes data)
docker-compose down -v
```

### Option 2: Docker Only

**Build Image**
```bash
docker build -t n8n-founderstories:latest .
```

**Run Container**
```bash
docker run -d \
  --name n8n-founderstories \
  -p 8000:8000 \
  --env-file .env \
  -v $(pwd)/credentials:/app/credentials:ro \
  -v n8n-logs:/app/logs \
  -v n8n-crawl-profile:/app/crawl4ai-profile \
  --restart unless-stopped \
  n8n-founderstories:latest
```

**Manage Container**
```bash
# View logs
docker logs -f n8n-founderstories

# Stop container
docker stop n8n-founderstories

# Start container
docker start n8n-founderstories

# Restart container
docker restart n8n-founderstories

# Remove container
docker rm -f n8n-founderstories
```

## 🖥️ Manual Deployment

### System Requirements
- Ubuntu 20.04+ or similar Linux distribution
- Python 3.10+
- PostgreSQL 12+
- 4GB+ RAM
- 20GB+ disk space

### Step 1: System Setup

```bash
# Update system
sudo apt update && sudo apt upgrade -y

# Install system dependencies
sudo apt install -y \
  python3.11 \
  python3.11-venv \
  python3-pip \
  postgresql \
  postgresql-contrib \
  nginx \
  git \
  build-essential \
  tesseract-ocr \
  poppler-utils
```

### Step 2: Database Setup

```bash
# Switch to postgres user
sudo -u postgres psql

# Create database and user
CREATE DATABASE n8n_founderstories;
CREATE USER n8n_user WITH ENCRYPTED PASSWORD 'your-secure-password';
GRANT ALL PRIVILEGES ON DATABASE n8n_founderstories TO n8n_user;
\q
```

### Step 3: Application Setup

```bash
# Create application user
sudo useradd -m -s /bin/bash n8n

# Switch to application user
sudo su - n8n

# Clone repository
git clone <repository-url>
cd N8N-FounderStories

# Create virtual environment
python3.11 -m venv venv
source venv/bin/activate

# Install dependencies
pip install --upgrade pip
pip install -r requirements.txt

# Download spacy model
python -m spacy download en_core_web_sm

# Install Playwright browsers
playwright install chromium

# Configure environment
cp .env.example .env
nano .env  # Edit with your values

# Create directories
mkdir -p credentials logs crawl4ai-profile
# Place service-account.json in credentials/
```

### Step 4: Systemd Service

Create `/etc/systemd/system/n8n-founderstories.service`:

```ini
[Unit]
Description=N8N-FounderStories API Service
After=network.target postgresql.service
Requires=postgresql.service

[Service]
Type=simple
User=n8n
Group=n8n
WorkingDirectory=/home/n8n/N8N-FounderStories
Environment="PATH=/home/n8n/N8N-FounderStories/venv/bin"
ExecStart=/home/n8n/N8N-FounderStories/venv/bin/python -m n8n_founderstories
Restart=always
RestartSec=10
StandardOutput=append:/home/n8n/N8N-FounderStories/logs/app.log
StandardError=append:/home/n8n/N8N-FounderStories/logs/error.log

# Security
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=/home/n8n/N8N-FounderStories/logs
ReadWritePaths=/home/n8n/N8N-FounderStories/crawl4ai-profile

[Install]
WantedBy=multi-user.target
```

**Enable and start service:**
```bash
sudo systemctl daemon-reload
sudo systemctl enable n8n-founderstories
sudo systemctl start n8n-founderstories
sudo systemctl status n8n-founderstories
```

### Step 5: Nginx Reverse Proxy

Create `/etc/nginx/sites-available/n8n-founderstories`:

```nginx
server {
    listen 80;
    server_name your-domain.com;

    # Redirect HTTP to HTTPS
    return 301 https://$server_name$request_uri;
}

server {
    listen 443 ssl http2;
    server_name your-domain.com;

    # SSL Configuration
    ssl_certificate /etc/letsencrypt/live/your-domain.com/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/your-domain.com/privkey.pem;
    ssl_protocols TLSv1.2 TLSv1.3;
    ssl_ciphers HIGH:!aNULL:!MD5;

    # Logging
    access_log /var/log/nginx/n8n-founderstories-access.log;
    error_log /var/log/nginx/n8n-founderstories-error.log;

    # Proxy settings
    location / {
        proxy_pass http://127.0.0.1:8000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        
        # Timeouts for long-running requests
        proxy_connect_timeout 300s;
        proxy_send_timeout 300s;
        proxy_read_timeout 300s;
    }

    # Rate limiting
    limit_req_zone $binary_remote_addr zone=api_limit:10m rate=10r/s;
    limit_req zone=api_limit burst=20 nodelay;
}
```

**Enable site:**
```bash
sudo ln -s /etc/nginx/sites-available/n8n-founderstories /etc/nginx/sites-enabled/
sudo nginx -t
sudo systemctl reload nginx
```

## 🔒 SSL Certificate (Let's Encrypt)

```bash
# Install certbot
sudo apt install certbot python3-certbot-nginx

# Obtain certificate
sudo certbot --nginx -d your-domain.com

# Auto-renewal is configured automatically
# Test renewal
sudo certbot renew --dry-run
```

## 📊 Monitoring & Logging

### Application Logs
```bash
# Systemd service logs
sudo journalctl -u n8n-founderstories -f

# Application log files
tail -f /home/n8n/N8N-FounderStories/logs/app.log
tail -f /home/n8n/N8N-FounderStories/logs/error.log
```

### Database Monitoring
```bash
# PostgreSQL logs
sudo tail -f /var/log/postgresql/postgresql-15-main.log

# Active connections
sudo -u postgres psql -c "SELECT count(*) FROM pg_stat_activity;"

# Database size
sudo -u postgres psql -c "SELECT pg_size_pretty(pg_database_size('n8n_founderstories'));"
```

### System Resources
```bash
# CPU and memory usage
htop

# Disk usage
df -h

# Network connections
netstat -tulpn | grep 8000
```

## 🔄 Updates & Maintenance

### Application Updates
```bash
# Stop service
sudo systemctl stop n8n-founderstories

# Backup database
sudo -u postgres pg_dump n8n_founderstories > backup_$(date +%Y%m%d).sql

# Update code
cd /home/n8n/N8N-FounderStories
git pull origin main

# Update dependencies
source venv/bin/activate
pip install -r requirements.txt --upgrade

# Restart service
sudo systemctl start n8n-founderstories
sudo systemctl status n8n-founderstories
```

### Database Maintenance
```bash
# Vacuum database
sudo -u postgres psql -d n8n_founderstories -c "VACUUM ANALYZE;"

# Reindex
sudo -u postgres psql -d n8n_founderstories -c "REINDEX DATABASE n8n_founderstories;"
```

## 🔐 Security Best Practices

1. **Firewall Configuration**
```bash
# Allow SSH, HTTP, HTTPS
sudo ufw allow 22/tcp
sudo ufw allow 80/tcp
sudo ufw allow 443/tcp
sudo ufw enable
```

2. **Database Security**
- Use strong passwords
- Restrict PostgreSQL to localhost
- Regular backups
- Enable SSL for database connections

3. **Application Security**
- Keep dependencies updated
- Use environment variables for secrets
- Implement rate limiting
- Regular security audits

4. **System Security**
- Keep system updated
- Use SSH keys instead of passwords
- Disable root login
- Configure fail2ban

## 📦 Backup Strategy

### Automated Backups
Create `/home/n8n/backup.sh`:

```bash
#!/bin/bash
BACKUP_DIR="/home/n8n/backups"
DATE=$(date +%Y%m%d_%H%M%S)

# Create backup directory
mkdir -p $BACKUP_DIR

# Backup database
sudo -u postgres pg_dump n8n_founderstories | gzip > $BACKUP_DIR/db_$DATE.sql.gz

# Backup credentials
tar -czf $BACKUP_DIR/credentials_$DATE.tar.gz /home/n8n/N8N-FounderStories/credentials/

# Keep only last 7 days
find $BACKUP_DIR -name "*.gz" -mtime +7 -delete

echo "Backup completed: $DATE"
```

**Schedule with cron:**
```bash
# Edit crontab
crontab -e

# Add daily backup at 2 AM
0 2 * * * /home/n8n/backup.sh >> /home/n8n/backup.log 2>&1
```

## 🚨 Troubleshooting

### Service Won't Start
```bash
# Check logs
sudo journalctl -u n8n-founderstories -n 50

# Check configuration
python -m n8n_founderstories --help

# Verify environment
cat .env | grep -v PASSWORD
```

### Database Connection Issues
```bash
# Test connection
psql -h localhost -U n8n_user -d n8n_founderstories

# Check PostgreSQL status
sudo systemctl status postgresql

# View PostgreSQL logs
sudo tail -f /var/log/postgresql/postgresql-15-main.log
```

### High Memory Usage
```bash
# Check process memory
ps aux | grep python | sort -k4 -r

# Adjust worker settings in .env
DOMAIN_CONCURRENCY=2
CRAWL4AI_MAX_CONCURRENCY=2
LLM_MAX_CONCURRENCY=3
```

## 📞 Support

For deployment issues:
- Check logs first
- Review this guide
- Consult README.md
- Open GitHub issue with logs

## ✅ Post-Deployment Checklist

- [ ] Application is accessible via HTTPS
- [ ] Health endpoint returns 200 OK
- [ ] Database connections are working
- [ ] Background workers are running
- [ ] Logs are being written correctly
- [ ] Backups are configured and tested
- [ ] Monitoring is set up
- [ ] SSL certificate auto-renewal is working
- [ ] Firewall rules are configured
- [ ] Documentation is updated with deployment details