# Atlan Data Extractor - Network Configuration Guide

## Current Issue: IP Address Restrictions

The Atlan Data Extractor is encountering network access restrictions when connecting to your Atlan instance. This is a common security measure implemented by organizations to protect their data APIs.

### Error Details

**Current Error:** `IP address not allowed: 35.229.42.2`

This indicates that your organization's Atlan instance has IP allowlisting enabled, and Replit's infrastructure IP (35.229.42.2) is not included in the allowed list.

## Solution Options

### Option 1: IP Allowlisting (Recommended for Production)
Contact your Atlan administrator to add Replit's IP address to the allowlist:
- **IP to Allowlist:** `35.229.42.2`
- **Ports:** 443 (HTTPS)
- **Purpose:** Data extraction via Atlan API

### Option 2: VPN/Proxy Access
If your organization uses VPN access for external tools:
1. Configure a VPN endpoint that has access to your Atlan instance
2. Route Replit traffic through the approved network path

### Option 3: Internal Network Deployment
Deploy the extractor within your organization's network infrastructure where Atlan access is already permitted.

### Option 4: Alternative Authentication
Some Atlan instances support different authentication methods:
- Service account tokens with broader IP access
- API gateway endpoints with different security policies

## Configuration Updates Required

Once network access is resolved, update your configuration:

1. **Set your Atlan instance URL:**
   ```json
   "atlan_instance_url": "https://your-org.atlan.com"
   ```

2. **Verify API endpoints:**
   - Connections: `/api/getConnections`
   - Databases: `/api/getDatabases`

3. **Authentication token:**
   - Use environment variable: `ATLAN_AUTH_TOKEN`
   - Or update config.json with valid token

## Testing Connectivity

After network access is configured, the extractor will:
1. Connect to your Atlan instance
2. Fetch connections data
3. For each connection, retrieve associated databases
4. Export data to CSV files in the `output/` directory

## Support

If you need assistance with:
- Obtaining proper credentials
- Understanding your Atlan instance configuration
- Network access requirements

Contact your organization's Atlan administrator or IT security team for network access configuration.