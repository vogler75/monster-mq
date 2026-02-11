#!/usr/bin/env bash
#
# Test MQTT v5 Phase 5 Message Expiry with all database backends
# This script:
# 1. Starts test databases using docker-compose-test.yml
# 2. Runs MonsterMQ broker with each database configuration
# 3. Executes Phase 5 message expiry tests for each backend
# 4. Generates a comprehensive test report
#

set -e

# Configuration
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BROKER_DIR="$PROJECT_ROOT/broker"
TESTS_DIR="$PROJECT_ROOT/tests"
TEST_SCRIPT="${TEST_SCRIPT:-test_mqtt5_phase5_message_expiry.py}"
SKIP_DOCKER_START=${SKIP_DOCKER_START:-false}
KEEP_DATABASES=${KEEP_DATABASES:-false}

# Color codes
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Output functions
function write_header() {
    echo -e "\n${CYAN}======================================================================${NC}"
    echo -e "${CYAN}$1${NC}"
    echo -e "${CYAN}======================================================================${NC}\n"
}

function write_success() {
    echo -e "${GREEN}✓ $1${NC}"
}

function write_error() {
    echo -e "${RED}✗ $1${NC}"
}

function write_info() {
    echo -e "${YELLOW}ℹ $1${NC}"
}

# Test results tracking
declare -A TEST_RESULTS
declare -A TEST_DURATIONS

# Start databases
if [ "$SKIP_DOCKER_START" != "true" ]; then
    write_header "STARTING TEST DATABASES"
    write_info "Starting PostgreSQL, CrateDB, and MongoDB..."
    
    cd "$PROJECT_ROOT"
    docker-compose -f docker-compose-test.yml up -d
    write_success "Databases started"
    
    write_info "Waiting for databases to be ready..."
    sleep 15
    
    # Check health status
    for container in monstermq-test-postgres monstermq-test-cratedb monstermq-test-mongodb; do
        health=$(docker inspect --format='{{.State.Health.Status}}' "$container" 2>/dev/null || echo "unknown")
        if [ "$health" == "healthy" ]; then
            write_success "$container is healthy"
        else
            write_info "$container status: $health (may still be starting)"
        fi
    done
    
    sleep 10
else
    write_info "Skipping database startup (using existing containers)"
fi

# Test function for a specific database
function test_database() {
    local db_name=$1
    local config_file=$2
    
    write_header "TESTING: $db_name"
    
    local start_time=$(date +%s)
    local broker_pid=""
    local test_output=""
    local test_success=false
    
    # Copy config file
    local config_path="$BROKER_DIR/configs/$config_file"
    local target_config="$BROKER_DIR/config.yaml"
    
    if [ ! -f "$config_path" ]; then
        write_error "Config file not found: $config_path"
        TEST_RESULTS[$db_name]="FAILED"
        return 1
    fi
    
    cp "$config_path" "$target_config"
    write_info "Using config: $config_file"
    
    # Stop any existing broker
    write_info "Stopping any existing broker..."
    pkill -9 -f "monster.*broker" 2>/dev/null || true
    sleep 2
    
    # Start broker
    write_info "Starting MonsterMQ broker with $db_name..."
    cd "$BROKER_DIR"
    ./run.sh > /dev/null 2>&1 &
    broker_pid=$!
    sleep 8  # Wait for broker to initialize
    
    if ! ps -p $broker_pid > /dev/null 2>&1; then
        write_error "Broker failed to start"
        TEST_RESULTS[$db_name]="FAILED"
        return 1
    fi
    
    write_success "Broker started (PID: $broker_pid)"
    
    # Run test
    write_info "Running Phase 5 message expiry test..."
    cd "$TESTS_DIR"
    
    if python3 "$TEST_SCRIPT"; then
        test_success=true
        write_success "$db_name test PASSED"
        TEST_RESULTS[$db_name]="PASSED"
    else
        write_error "$db_name test FAILED"
        TEST_RESULTS[$db_name]="FAILED"
    fi
    
    # Stop broker
    write_info "Stopping broker..."
    kill -9 $broker_pid 2>/dev/null || true
    pkill -9 -f "monster.*broker" 2>/dev/null || true
    sleep 2
    
    local end_time=$(date +%s)
    local duration=$((end_time - start_time))
    TEST_DURATIONS[$db_name]=$duration
    
    return $([ "$test_success" == "true" ] && echo 0 || echo 1)
}

# Run tests for each database
write_header "MQTT V5.0 PHASE 5 - DATABASE BACKEND TESTING"
write_info "Testing Message Expiry with PostgreSQL, CrateDB, and MongoDB"

test_database "PostgreSQL" "test-postgres.yaml" || true
sleep 3

test_database "CrateDB" "test-cratedb.yaml" || true
sleep 3

test_database "MongoDB" "test-mongodb.yaml" || true

# Generate summary report
write_header "TEST SUMMARY"

all_passed=true
for db in PostgreSQL CrateDB MongoDB; do
    result="${TEST_RESULTS[$db]}"
    duration="${TEST_DURATIONS[$db]}"
    
    if [ "$result" == "PASSED" ]; then
        echo -e "${GREEN}$db : ✓ PASSED (${duration}s)${NC}"
    else
        echo -e "${RED}$db : ✗ FAILED (${duration}s)${NC}"
        all_passed=false
    fi
done

echo ""

if [ "$all_passed" == "true" ]; then
    write_success "ALL DATABASE TESTS PASSED!"
    echo ""
    write_header "✓✓✓ PHASE 5 VALIDATION COMPLETE ✓✓✓"
    write_info "PostgreSQL: VALIDATED ✓"
    write_info "CrateDB: VALIDATED ✓"
    write_info "MongoDB: VALIDATED ✓"
else
    write_error "SOME TESTS FAILED - Review output above"
fi

# Cleanup
if [ "$KEEP_DATABASES" != "true" ]; then
    echo ""
    write_info "Stopping test databases..."
    cd "$PROJECT_ROOT"
    docker-compose -f docker-compose-test.yml down -v
    write_success "Databases stopped and volumes removed"
else
    write_info "Keeping databases running"
    write_info "To stop: docker-compose -f docker-compose-test.yml down -v"
fi

echo ""

exit $([ "$all_passed" == "true" ] && echo 0 || echo 1)
