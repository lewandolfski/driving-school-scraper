#!/bin/bash

# Cloud Deployment Script for Driving School Scraper
# Supports GCP, DigitalOcean, and AWS

set -e

echo "🚀 DRIVING SCHOOL SCRAPER - CLOUD DEPLOYMENT"
echo "============================================="

# Check if cloud provider is specified
if [ -z "$1" ]; then
    echo "Usage: ./deploy.sh [gcp|digitalocean|aws]"
    echo ""
    echo "Recommended options:"
    echo "  gcp          - Google Cloud Platform (Compute Engine + Cloud SQL)"
    echo "  digitalocean - DigitalOcean Droplet + Managed Database"
    echo "  aws          - AWS EC2 + RDS"
    exit 1
fi

CLOUD_PROVIDER=$1

# Generate random password for database
DB_PASSWORD=$(openssl rand -base64 32)
API_KEY=$(openssl rand -base64 32)

# Create environment file
cat > .env << EOF
DB_PASSWORD=${DB_PASSWORD}
API_KEY=${API_KEY}
CLOUD_PROVIDER=${CLOUD_PROVIDER}
EOF

echo "📝 Created environment configuration"

case $CLOUD_PROVIDER in
    "gcp")
        echo "🌐 Setting up Google Cloud Platform deployment..."
        
        # Create GCP deployment script
        cat > deploy-gcp.sh << 'EOF'
#!/bin/bash

# GCP Deployment
PROJECT_ID="driving-schools-scraper"
REGION="europe-west1"
ZONE="europe-west1-b"

echo "Creating GCP project and resources..."

# Create VM instance
gcloud compute instances create driving-schools-vm \
    --project=$PROJECT_ID \
    --zone=$ZONE \
    --machine-type=e2-standard-2 \
    --boot-disk-size=50GB \
    --boot-disk-type=pd-standard \
    --image-family=ubuntu-2004-lts \
    --image-project=ubuntu-os-cloud \
    --tags=http-server,https-server

# Create Cloud SQL instance
gcloud sql instances create driving-schools-db \
    --project=$PROJECT_ID \
    --database-version=POSTGRES_15 \
    --tier=db-f1-micro \
    --region=$REGION \
    --storage-size=20GB \
    --storage-type=SSD

# Create database and user
gcloud sql databases create driving_schools --instance=driving-schools-db
gcloud sql users create scraper_user --instance=driving-schools-db --password=$DB_PASSWORD

echo "✅ GCP resources created"
echo "📋 Next steps:"
echo "   1. SSH into VM: gcloud compute ssh driving-schools-vm --zone=$ZONE"
echo "   2. Clone repository and run: docker-compose -f docker-compose.cloud.yml up -d"
echo "   3. Update DATABASE_URL with Cloud SQL connection string"
EOF
        
        chmod +x deploy-gcp.sh
        echo "📁 Created deploy-gcp.sh - run this script to deploy to GCP"
        echo "💰 Estimated cost: $25-35/month"
        ;;
        
    "digitalocean")
        echo "🌊 Setting up DigitalOcean deployment..."
        
        cat > deploy-digitalocean.sh << 'EOF'
#!/bin/bash

echo "Creating DigitalOcean resources..."

# Create droplet
doctl compute droplet create driving-schools-scraper \
    --size s-2vcpu-2gb \
    --image ubuntu-20-04-x64 \
    --region ams3 \
    --ssh-keys $(doctl compute ssh-key list --format ID --no-header | head -1) \
    --wait

# Create managed database
doctl databases create driving-schools-db \
    --engine postgres \
    --version 15 \
    --size db-s-1vcpu-1gb \
    --region ams3 \
    --num-nodes 1

echo "✅ DigitalOcean resources created"
echo "📋 Next steps:"
echo "   1. SSH into droplet: doctl compute ssh driving-schools-scraper"
echo "   2. Install Docker and Docker Compose"
echo "   3. Clone repository and run deployment"
EOF
        
        chmod +x deploy-digitalocean.sh
        echo "📁 Created deploy-digitalocean.sh - run this script to deploy to DigitalOcean"
        echo "💰 Estimated cost: $21/month ($6 droplet + $15 database)"
        ;;
        
    "aws")
        echo "☁️ Setting up AWS deployment..."
        
        cat > deploy-aws.sh << 'EOF'
#!/bin/bash

echo "Creating AWS resources..."

# Create EC2 instance
aws ec2 run-instances \
    --image-id ami-0c02fb55956c7d316 \
    --count 1 \
    --instance-type t3.micro \
    --key-name your-key-pair \
    --security-group-ids sg-your-security-group \
    --subnet-id subnet-your-subnet

# Create RDS instance
aws rds create-db-instance \
    --db-instance-identifier driving-schools-db \
    --db-instance-class db.t3.micro \
    --engine postgres \
    --master-username scraper_user \
    --master-user-password $DB_PASSWORD \
    --allocated-storage 20 \
    --db-name driving_schools

echo "✅ AWS resources created"
echo "📋 Configure security groups and VPC settings"
EOF
        
        chmod +x deploy-aws.sh
        echo "📁 Created deploy-aws.sh - run this script to deploy to AWS"
        echo "💰 Estimated cost: $20-25/month"
        ;;
        
    *)
        echo "❌ Unknown cloud provider: $CLOUD_PROVIDER"
        exit 1
        ;;
esac

echo ""
echo "🔧 DEPLOYMENT SETUP COMPLETE"
echo "================================"
echo ""
echo "📋 What was created:"
echo "   • Docker configuration for cloud deployment"
echo "   • Production scraper with PostgreSQL support"
echo "   • FastAPI backend for data access"
echo "   • Cloud-specific deployment scripts"
echo ""
echo "💡 Recommended: DigitalOcean (most cost-effective at $21/month)"
echo ""
echo "🚀 Next steps:"
echo "   1. Choose your cloud provider and run the deployment script"
echo "   2. The scraper will run 24/7 and complete in ~40-50 hours"
echo "   3. Access your data via API at http://your-server:8000/api/schools"
echo "   4. Build your comparison website using the API"
