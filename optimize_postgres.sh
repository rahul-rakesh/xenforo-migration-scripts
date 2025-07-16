#!/bin/bash

set -e

#make executable - chmod +x optimize_postgres.sh
#run it - sudo ./optimize_postgres.sh

CONF_FILE="/etc/postgresql/15/main/postgresql.conf"
PG_USER="postgres"
PG_DATA_DIR="/var/lib/postgresql/15/main"  # Adjust if different

echo "🛑 Stopping PostgreSQL..."
sudo sv stop postgres || true
sleep 3

echo "⚠️ Killing remaining postgres processes..."
sudo pkill -u $PG_USER || true
sleep 2

echo "🧹 Removing stale postmaster.pid..."
sudo rm -f "$PG_DATA_DIR/postmaster.pid"

echo "🚀 Applying high-performance PostgreSQL tuning..."

sudo sed -i "s/^#*\s*shared_buffers\s*=.*/shared_buffers = 4GB/" "$CONF_FILE" || echo "shared_buffers = 4GB" | sudo tee -a "$CONF_FILE"
sudo sed -i "s/^#*\s*work_mem\s*=.*/work_mem = 64MB/" "$CONF_FILE" || echo "work_mem = 64MB" | sudo tee -a "$CONF_FILE"
sudo sed -i "s/^#*\s*maintenance_work_mem\s*=.*/maintenance_work_mem = 2GB/" "$CONF_FILE" || echo "maintenance_work_mem = 2GB" | sudo tee -a "$CONF_FILE"
sudo sed -i "s/^#*\s*effective_cache_size\s*=.*/effective_cache_size = 12GB/" "$CONF_FILE" || echo "effective_cache_size = 12GB" | sudo tee -a "$CONF_FILE"
sudo sed -i "s/^#*\s*wal_buffers\s*=.*/wal_buffers = 16MB/" "$CONF_FILE" || echo "wal_buffers = 16MB" | sudo tee -a "$CONF_FILE"
sudo sed -i "s/^#*\s*max_wal_size\s*=.*/max_wal_size = 2GB/" "$CONF_FILE" || echo "max_wal_size = 2GB" | sudo tee -a "$CONF_FILE"

echo "🚀 Restarting PostgreSQL..."
sudo sv start postgres
sleep 5

echo "🔍 Verifying settings..."
sudo -u $PG_USER psql -U $PG_USER -c "SHOW shared_buffers;"
sudo -u $PG_USER psql -U $PG_USER -c "SHOW work_mem;"
sudo -u $PG_USER psql -U $PG_USER -c "SHOW maintenance_work_mem;"
sudo -u $PG_USER psql -U $PG_USER -c "SHOW effective_cache_size;"

echo "✅ PostgreSQL optimized for high-performance bulk operations."