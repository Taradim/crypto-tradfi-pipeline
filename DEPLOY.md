# Deploy to Raspberry Pi

## Prerequisites

1. **AWS S3 Bucket**: Make sure you've created the S3 bucket using Terraform:
   ```bash
   cd terraform
   terraform apply
   ```

2. **AWS Credentials**: You need your AWS credentials (from `data-pipeline-user` or the IAM user created by Terraform)

## Complete Installation on Raspberry Pi (from scratch)

### Step 1: Connect to Raspberry Pi

From your Mac:
```bash
ssh pi@<PI_IP>
# Example: ssh pi@192.168.1.100
```

### Step 2: Install Prerequisites

On the Raspberry Pi, run these commands:

```bash
# Update the system
sudo apt update && sudo apt upgrade -y

# Install Git
sudo apt install git -y

# Install Docker
curl -fsSL https://get.docker.com -o get-docker.sh
sudo sh get-docker.sh

# Add user pi to docker group (to avoid sudo)
sudo usermod -aG docker pi

# Docker Compose is included as a plugin with modern Docker installations
# Verify installations
docker --version
docker compose version
git --version
```

**Important**: After adding the user to the docker group, disconnect and reconnect for the changes to take effect:
```bash
exit
# Then reconnect
ssh pi@<PI_IP>
```

### Step 3: Clone the Repository

```bash
# Clone the repo
git clone <YOUR_REPO_URL> ~/crypto-pipeline
cd ~/crypto-pipeline

# Verify files are present
ls -la
```

### Step 4: Configure Environment

```bash
# Create .env.airflow file from example
cp env.airflow.example .env.airflow

# Edit the file with your credentials
nano .env.airflow
```

In the `.env.airflow` file, modify these values:
- `S3_BUCKET_NAME=coding-projects-taradim-2026` (or your bucket name)
- `AWS_ACCESS_KEY_ID=your-access-key-id`
- `AWS_SECRET_ACCESS_KEY=your-secret-access-key`
- `AWS_DEFAULT_REGION=eu-west-1`
- `COINGECKO_API_KEY=your-api-key` (optional but recommended)
- `AIRFLOW_WWW_USER_PASSWORD=your-secure-password` (change the default password!)

To save in nano: `Ctrl+O` then `Enter`, then `Ctrl+X` to exit.

### Step 5: Configure Airflow UID

```bash
# Set Airflow UID (required for permissions)
export AIRFLOW_UID=$(id -u)

# To make this variable permanent, add it to .bashrc
echo "export AIRFLOW_UID=$(id -u)" >> ~/.bashrc
```

### Step 6: Start Services

```bash
# Start Docker Compose in background (note: use "docker compose" with space, not "docker-compose")
docker compose up -d

# Verify containers are starting correctly
docker compose ps

# View logs to check for errors
docker compose logs -f
# Press Ctrl+C to exit logs
```

### Step 7: Access Airflow Interface

1. Open your browser on your Mac
2. Go to: `http://<PI_IP>:8080`
3. Login with:
   - Username: `airflow`
   - Password: (the one you set in `.env.airflow`)

## Future Updates

To update the code after changes:
```bash
cd ~/crypto-pipeline
git pull
docker compose up -d --build
```

## Useful Commands

```bash
# View service logs
docker compose logs -f

# Stop services
docker compose down

# Restart services
docker compose restart

# View container status
docker compose ps

# Clean volumes (⚠️ deletes data)
docker compose down -v
```

## Troubleshooting

### Docker Compose package conflict

If you see errors about `docker-buildx` or `docker-compose` package conflicts, Docker Compose is already installed as a plugin. Use `docker compose` (with space) instead of `docker-compose` (with hyphen):

```bash
# Use this (plugin version - modern)
docker compose up -d

# NOT this (old standalone package)
docker-compose up -d
```

### Containers won't start
```bash
# Check logs
docker compose logs

# Verify Docker is working
docker ps

# Check permissions
ls -la .env.airflow
```

### Docker permissions issue
```bash
# Verify you're in the docker group
groups

# If not in the group, reconnect after running:
sudo usermod -aG docker pi
```

### Airflow not accessible
- Verify port 8080 is not blocked by firewall
- Check Pi's IP: `hostname -I`
- Verify containers are running: `docker compose ps`
