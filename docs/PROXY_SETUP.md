# Proxy Configuration Guide

The scraper supports HTTP/HTTPS proxy configuration to route traffic through a different IP address.

## Setup

### 1. Configure Proxy in .env

Edit `.env` and uncomment/configure the proxy lines:

```bash
# Without authentication
HTTP_PROXY=http://proxy-server.com:8080
HTTPS_PROXY=http://proxy-server.com:8080

# With authentication
HTTP_PROXY=http://username:password@proxy-server.com:8080
HTTPS_PROXY=http://username:password@proxy-server.com:8080
```

### 2. Restart Docker Container

```bash
docker-compose restart app
```

### 3. Verify New IP

Check what IP the scraper is now using:

```bash
docker-compose exec app python -c "import requests; print(requests.get('https://api.ipify.org', proxies={'http': 'http://your-proxy:port', 'https': 'http://your-proxy:port'}).text)"
```

## Proxy Service Providers

### Free/Testing
- **Your organization's proxy** - Check with IT department
- **Squid proxy** - Self-hosted on a different server

### Commercial (Paid)
- **Bright Data** (formerly Luminati) - https://brightdata.com
- **Oxylabs** - https://oxylabs.io
- **ScraperAPI** - https://scraperapi.com
- **Smartproxy** - https://smartproxy.com

### Recommendations
- **Datacenter proxies**: Fast and cheap, good for non-blocked sites
- **Residential proxies**: More expensive, less likely to be blocked (looks like regular users)
- **Rotating proxies**: Automatic IP rotation per request

## Example: Using Bright Data

```bash
# Add to .env
HTTP_PROXY=http://your-username-route-residential:your-password@brd.superproxy.io:22225
HTTPS_PROXY=http://your-username-route-residential:your-password@brd.superproxy.io:22225
```

## Troubleshooting

### Proxy Connection Errors
```
ProxyError: Cannot connect to proxy
```
- Verify proxy URL and port are correct
- Check if proxy requires authentication
- Test proxy connection directly: `curl -x http://proxy:port https://api.ipify.org`

### Timeout Errors
```
ReadTimeout: Request timed out
```
- Proxy may be slow or overloaded
- Increase timeout in `backend/scraper_service.py` (currently 30s)

### Still Getting Blocked
- Try residential proxies instead of datacenter proxies
- Enable rotating proxies (different IP per request)
- Slow down request rate (reduce parallel_workers parameter)
- Contact IT to whitelist the proxy IP range

## Disable Proxy

Comment out or remove the proxy lines from `.env`:

```bash
#HTTP_PROXY=http://proxy-server:port
#HTTPS_PROXY=http://proxy-server:port
```

Then restart: `docker-compose restart app`
