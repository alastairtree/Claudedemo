#!/bin/bash
# Generate and serve crump documentation locally
#
# Usage:
#   ./generate-docs.sh          # Serve documentation with live reload
#   ./generate-docs.sh build    # Build static site to site/ directory
#   ./generate-docs.sh help     # Show this help message

set -e

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Check if mkdocs is installed
check_mkdocs() {
    if ! command -v mkdocs &> /dev/null; then
        echo -e "${YELLOW}MkDocs not found. Installing dependencies...${NC}"

        # Check for uv first, fall back to pip
        if command -v uv &> /dev/null; then
            echo "Using uv to install dependencies..."
            uv sync --all-extras
        elif command -v pip &> /dev/null; then
            echo "Using pip to install dependencies..."
            pip install -e ".[dev]"
        else
            echo -e "${YELLOW}Error: Neither uv nor pip found. Please install one of them.${NC}"
            exit 1
        fi

        echo -e "${GREEN}✓ Dependencies installed${NC}"
    fi
}

# Show help message
show_help() {
    echo "Generate and serve crump documentation"
    echo ""
    echo "Usage:"
    echo "  ./generate-docs.sh          Serve documentation with live reload (default)"
    echo "  ./generate-docs.sh build    Build static site to site/ directory"
    echo "  ./generate-docs.sh help     Show this help message"
    echo ""
    echo "Examples:"
    echo "  ./generate-docs.sh                    # Start local server at http://127.0.0.1:8000"
    echo "  ./generate-docs.sh build              # Build docs to site/"
    echo ""
    echo "Requirements:"
    echo "  - Python 3.11+"
    echo "  - MkDocs with Material theme (installed via dev dependencies)"
    echo ""
    echo "Note: On first run, this script will automatically install required dependencies."
}

# Build static site
build_docs() {
    echo -e "${BLUE}Building documentation...${NC}"

    if command -v uv &> /dev/null; then
        uv run mkdocs build --clean
    else
        mkdocs build --clean
    fi

    echo -e "${GREEN}✓ Documentation built to site/ directory${NC}"
    echo ""
    echo "To view the built site:"
    echo "  cd site && python -m http.server 8000"
}

# Serve documentation with live reload
serve_docs() {
    echo -e "${BLUE}Starting documentation server...${NC}"
    echo ""
    echo -e "${GREEN}Documentation will be available at: http://127.0.0.1:8000${NC}"
    echo ""
    echo "Press Ctrl+C to stop the server"
    echo ""

    if command -v uv &> /dev/null; then
        uv run mkdocs serve
    else
        mkdocs serve
    fi
}

# Main script
main() {
    # Check if mkdocs is installed
    check_mkdocs

    # Parse command
    case "${1:-serve}" in
        build)
            build_docs
            ;;
        serve)
            serve_docs
            ;;
        help|--help|-h)
            show_help
            ;;
        *)
            echo -e "${YELLOW}Unknown command: $1${NC}"
            echo ""
            show_help
            exit 1
            ;;
    esac
}

main "$@"
