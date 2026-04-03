#!/bin/bash

# Thema Ads Optimizer - Docker Helper Script
# This script helps you run the optimizer in Docker

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Print colored message
print_message() {
    local color=$1
    local message=$2
    echo -e "${color}${message}${NC}"
}

# Check if Docker is running
check_docker() {
    if ! docker info > /dev/null 2>&1; then
        print_message "$RED" "Error: Docker is not running. Please start Docker Desktop."
        exit 1
    fi
    print_message "$GREEN" "✓ Docker is running"
}

# Check if .env file exists
check_env() {
    if [ ! -f .env ]; then
        print_message "$RED" "Error: .env file not found!"
        print_message "$YELLOW" "Creating .env from .env.example..."
        cp .env.example .env
        print_message "$YELLOW" "Please edit .env with your Google Ads credentials before running."
        exit 1
    fi
    print_message "$GREEN" "✓ .env file found"
}

# Create required directories
setup_directories() {
    mkdir -p data logs
    print_message "$GREEN" "✓ Created data/ and logs/ directories"
}

# Build Docker image
build_image() {
    print_message "$YELLOW" "Building Docker image..."
    docker-compose build
    print_message "$GREEN" "✓ Docker image built successfully"
}

# Run in dry-run mode
dry_run() {
    print_message "$YELLOW" "Running in DRY-RUN mode (no changes will be made)..."
    docker-compose run --rm -e DRY_RUN=true thema-ads-optimizer
}

# Run in production mode
run_prod() {
    print_message "$YELLOW" "Running in PRODUCTION mode..."
    docker-compose run --rm thema-ads-optimizer
}

# View logs
view_logs() {
    if [ -f logs/thema_ads_optimized.log ]; then
        tail -f logs/thema_ads_optimized.log
    else
        print_message "$RED" "No log file found yet."
    fi
}

# Clean up
cleanup() {
    print_message "$YELLOW" "Cleaning up Docker resources..."
    docker-compose down
    print_message "$GREEN" "✓ Cleanup complete"
}

# Show help
show_help() {
    cat << EOF
Thema Ads Optimizer - Docker Helper

Usage: ./docker-run.sh [command]

Commands:
    setup       Setup environment (create directories, check .env)
    build       Build Docker image
    dry-run     Run in dry-run mode (preview only, no changes)
    run         Run in production mode (makes actual changes)
    logs        View application logs
    clean       Clean up Docker resources
    help        Show this help message

Examples:
    ./docker-run.sh setup
    ./docker-run.sh build
    ./docker-run.sh dry-run
    ./docker-run.sh run

Before running:
1. Ensure Docker Desktop is running
2. Copy .env.example to .env and configure your credentials
3. Place your input file (Excel/CSV) in the data/ directory
EOF
}

# Main script logic
main() {
    case "${1:-help}" in
        setup)
            check_docker
            setup_directories
            check_env
            print_message "$GREEN" "Setup complete! Next steps:"
            print_message "$YELLOW" "1. Edit .env with your Google Ads credentials"
            print_message "$YELLOW" "2. Place input file in data/ directory"
            print_message "$YELLOW" "3. Run: ./docker-run.sh build"
            ;;
        build)
            check_docker
            build_image
            print_message "$GREEN" "Build complete! Run with: ./docker-run.sh dry-run"
            ;;
        dry-run)
            check_docker
            check_env
            setup_directories
            dry_run
            ;;
        run)
            check_docker
            check_env
            setup_directories
            print_message "$RED" "WARNING: This will make actual changes to your Google Ads account!"
            read -p "Are you sure you want to continue? (yes/no): " confirm
            if [ "$confirm" == "yes" ]; then
                run_prod
            else
                print_message "$YELLOW" "Cancelled."
            fi
            ;;
        logs)
            view_logs
            ;;
        clean)
            cleanup
            ;;
        help|*)
            show_help
            ;;
    esac
}

main "$@"
