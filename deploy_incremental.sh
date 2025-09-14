#!/bin/bash

# Deploy incremental scraper to cloud VM
echo "🚀 Deploying incremental scraper with persistent background execution..."

# Stop existing containers
echo "📦 Stopping existing containers..."
docker-compose -f cloud_deployment/docker-compose.cloud.yml down

# Rebuild with latest changes
echo "🔨 Rebuilding containers with incremental scraper..."
docker-compose -f cloud_deployment/docker-compose.cloud.yml build --no-cache

# Start in detached mode (background)
echo "▶️ Starting scraper in detached mode..."
docker-compose -f cloud_deployment/docker-compose.cloud.yml up -d

# Show status
echo "📊 Container status:"
docker-compose -f cloud_deployment/docker-compose.cloud.yml ps

echo ""
echo "✅ Incremental scraper deployed successfully!"
echo ""
echo "📋 Key improvements:"
echo "   • Saves data to database immediately after each city"
echo "   • Resumes from where it left off if interrupted"
echo "   • Runs persistently in background (detached mode)"
echo "   • Graceful shutdown handling"
echo "   • Progress tracking in database"
echo ""
echo "🔍 Monitor progress:"
echo "   docker logs driving_school_scraper -f"
echo ""
echo "📊 Check database:"
echo "   docker exec -it driving_schools_db psql -U scraper_user -d driving_schools -c \"SELECT COUNT(*) FROM driving_schools;\""
echo ""
echo "🛑 Stop scraper:"
echo "   docker-compose -f cloud_deployment/docker-compose.cloud.yml down"
