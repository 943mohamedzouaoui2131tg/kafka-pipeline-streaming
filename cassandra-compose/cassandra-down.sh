#!/bin/bash

# Cassandra Cluster Cleanup Script
# This script removes all tables, keyspace, and optionally the entire cluster

set +e  # Don't exit on error, continue cleanup

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
KEYSPACE_NAME="Projet_bd_Rf3"
CONTAINERS=("cassandra1" "cassandra2" "cassandra3")
TABLES=("trips_by_borough_time" "trips_by_pickup_zone" "trips_by_route" "trips_by_vendor")

echo -e "${BLUE}=========================================="
echo "Cassandra Cluster Cleanup Script"
echo -e "==========================================${NC}\n"

# Function to print status messages
print_status() {
    echo -e "${GREEN}[✓]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[!]${NC} $1"
}

print_error() {
    echo -e "${RED}[✗]${NC} $1"
}

print_info() {
    echo -e "${BLUE}[i]${NC} $1"
}

# Check if cassandra1 (primary node) is running
check_cassandra() {
    if docker ps -q -f name=cassandra1 2>/dev/null | grep -q .; then
        return 0
    else
        return 1
    fi
}

# Display current state
echo -e "${BLUE}=== Current Cluster State ===${NC}"
print_info "Checking for Cassandra containers..."
RUNNING=0
for container in "${CONTAINERS[@]}"; do
    if docker ps -q -f name=$container 2>/dev/null | grep -q .; then
        echo -e "  ${GREEN}●${NC} $container (running)"
        RUNNING=$((RUNNING + 1))
    elif docker ps -aq -f name=$container 2>/dev/null | grep -q .; then
        echo -e "  ${YELLOW}●${NC} $container (stopped)"
    else
        echo -e "  ${RED}○${NC} $container (not found)"
    fi
done

echo ""

if [ $RUNNING -eq 0 ]; then
    print_warning "No Cassandra containers are running!"
    echo ""
    echo -e "${YELLOW}Do you want to:${NC}"
    echo "  1) Remove stopped containers and volumes"
    echo "  2) Exit"
    read -p "Enter choice (1-2): " choice
    
    if [ "$choice" != "1" ]; then
        echo "Exiting..."
        exit 0
    fi
    
    # Skip to container removal
    echo ""
    echo -e "${BLUE}=== Removing Stopped Containers ===${NC}"
    for container in "${CONTAINERS[@]}"; do
        if docker ps -aq -f name=$container 2>/dev/null | grep -q .; then
            docker rm $container 2>/dev/null && print_status "$container removed" || print_error "Failed to remove $container"
        fi
    done
    
    # Ask about volumes
    echo ""
    echo -e "${YELLOW}Do you want to remove Cassandra data volumes? (This will delete all data!)${NC}"
    read -p "Remove volumes? (y/N): " -n 1 -r
    echo
    
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        echo -e "${BLUE}=== Removing Data Volumes ===${NC}"
        docker volume ls | grep cassandra | awk '{print $2}' | while read volume; do
            docker volume rm $volume 2>/dev/null && print_status "$volume removed" || print_warning "$volume not found or in use"
        done
    fi
    
    echo ""
    print_status "Cleanup complete!"
    exit 0
fi

# Cassandra is running, proceed with data cleanup
echo -e "${YELLOW}Choose cleanup option:${NC}"
echo "  1) Drop all tables (keep keyspace structure)"
echo "  2) Drop entire keyspace (including all tables)"
echo "  3) Drop keyspace + stop containers"
echo "  4) Complete cleanup (keyspace + containers + volumes)"
echo "  5) Just stop containers (keep data)"
echo "  6) Cancel"
read -p "Enter choice (1-6): " choice

case $choice in
    1)
        echo ""
        echo -e "${BLUE}=== Dropping All Tables ===${NC}"
        for table in "${TABLES[@]}"; do
            echo "Dropping table: $table"
            docker exec -i cassandra1 cqlsh <<EOF
USE $KEYSPACE_NAME;
DROP TABLE IF EXISTS $table;
EOF
            if [ $? -eq 0 ]; then
                print_status "$table dropped successfully"
            else
                print_error "Failed to drop $table"
            fi
        done
        
        echo ""
        print_info "Verifying remaining tables..."
        docker exec -i cassandra1 cqlsh <<EOF
USE $KEYSPACE_NAME;
DESCRIBE TABLES;
EOF
        ;;
        
    2)
        echo ""
        echo -e "${BLUE}=== Dropping Keyspace: $KEYSPACE_NAME ===${NC}"
        print_warning "This will delete ALL tables and data in the keyspace!"
        read -p "Are you sure? (y/N): " -n 1 -r
        echo
        
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            docker exec -i cassandra1 cqlsh <<EOF
DROP KEYSPACE IF EXISTS $KEYSPACE_NAME;
EOF
            if [ $? -eq 0 ]; then
                print_status "Keyspace $KEYSPACE_NAME dropped successfully"
            else
                print_error "Failed to drop keyspace"
            fi
            
            echo ""
            print_info "Remaining keyspaces:"
            docker exec -i cassandra1 cqlsh -e "DESCRIBE KEYSPACES;"
        else
            print_info "Cancelled keyspace drop"
        fi
        ;;
        
    3)
        echo ""
        echo -e "${BLUE}=== Dropping Keyspace and Stopping Containers ===${NC}"
        print_warning "This will delete the keyspace and stop all Cassandra containers!"
        read -p "Are you sure? (y/N): " -n 1 -r
        echo
        
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            # Drop keyspace first
            echo "Dropping keyspace..."
            docker exec -i cassandra1 cqlsh <<EOF
DROP KEYSPACE IF EXISTS $KEYSPACE_NAME;
EOF
            print_status "Keyspace dropped"
            
            # Stop containers
            echo ""
            echo "Stopping containers..."
            for container in "${CONTAINERS[@]}"; do
                docker stop $container 2>/dev/null && print_status "$container stopped" || print_warning "$container already stopped"
            done
        else
            print_info "Cancelled"
        fi
        ;;
        
    4)
        echo ""
        echo -e "${RED}=== COMPLETE CLEANUP ===${NC}"
        echo -e "${RED}WARNING: This will DELETE ALL DATA and remove containers!${NC}"
        read -p "Are you ABSOLUTELY sure? Type 'DELETE' to confirm: " confirm
        
        if [ "$confirm" = "DELETE" ]; then
            # Drop keyspace
            echo ""
            echo -e "${BLUE}Step 1/4: Dropping keyspace...${NC}"
            if check_cassandra; then
                docker exec -i cassandra1 cqlsh <<EOF
DROP KEYSPACE IF EXISTS $KEYSPACE_NAME;
EOF
                print_status "Keyspace dropped"
            else
                print_warning "Cassandra not running, skipping keyspace drop"
            fi
            
            # Stop containers
            echo ""
            echo -e "${BLUE}Step 2/4: Stopping containers...${NC}"
            for container in "${CONTAINERS[@]}"; do
                docker stop $container 2>/dev/null && print_status "$container stopped" || print_warning "$container already stopped"
            done
            
            # Remove containers
            echo ""
            echo -e "${BLUE}Step 3/4: Removing containers...${NC}"
            for container in "${CONTAINERS[@]}"; do
                docker rm $container 2>/dev/null && print_status "$container removed" || print_warning "$container already removed"
            done
            
            # Remove volumes
            echo ""
            echo -e "${BLUE}Step 4/4: Removing data volumes...${NC}"
            docker volume ls | grep cassandra | awk '{print $2}' | while read volume; do
                docker volume rm $volume 2>/dev/null && print_status "$volume removed" || print_warning "$volume not found"
            done
            
            # Remove network if exists
            echo ""
            if docker network ls | grep -q cassandra-network; then
                docker network rm cassandra-network 2>/dev/null && print_status "Network removed" || print_warning "Network removal failed"
            fi
            
            echo ""
            print_status "Complete cleanup finished!"
        else
            print_info "Cleanup cancelled"
        fi
        ;;
        
    5)
        echo ""
        echo -e "${BLUE}=== Stopping Containers (Data Preserved) ===${NC}"
        for container in "${CONTAINERS[@]}"; do
            docker stop $container 2>/dev/null && print_status "$container stopped" || print_warning "$container already stopped"
        done
        print_info "Data volumes preserved. Use 'docker start cassandra1 cassandra2 cassandra3' to restart"
        ;;
        
    6)
        print_info "Cleanup cancelled"
        exit 0
        ;;
        
    *)
        print_error "Invalid choice"
        exit 1
        ;;
esac

echo ""
echo -e "${BLUE}=== Cleanup Summary ===${NC}"
if check_cassandra; then
    print_info "Cassandra is still running"
    docker exec cassandra1 nodetool status 2>/dev/null || true
else
    print_info "Cassandra containers are stopped"
fi

echo ""
echo -e "${GREEN}=========================================="
echo "Cleanup Complete!"
echo -e "==========================================${NC}"
echo ""
echo "Useful commands:"
echo "  Check containers: docker ps -a | grep cassandra"
echo "  Check volumes:    docker volume ls | grep cassandra"
echo "  Restart cluster:  docker start cassandra1 cassandra2 cassandra3"
echo "  View logs:        docker logs cassandra1"
echo ""