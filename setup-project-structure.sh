#!/bin/bash
# setup-project-structure.sh
# Run this from your DataPipelineProject directory

echo "🏗️  Setting up Data Pipeline Project Structure..."

# Create all necessary directories
directories=(
    "docker/airflow"
    "docker/spark"
    "docker/app"
    "docker/postgres"
    "src/data_generator"
    "src/spark_jobs/streaming"
    "src/spark_jobs/batch"
    "src/kafka_setup"
    "src/monitoring"
    "dags"
    "data/raw"
    "data/processed"
    "data/lake/bronze"
    "data/lake/silver"
    "data/lake/gold"
    "config/kafka"
    "config/spark"
    "config/airflow"
    "logs/airflow"
    "logs/spark"
    "logs/app"
    "tests/unit"
    "tests/integration"
    "tests/e2e"
    "terraform"
    "k8s/airflow"
    "k8s/spark"
    "docs/architecture"
    "docs/operations"
    "plugins"
)

for dir in "${directories[@]}"; do
    mkdir -p "$dir"
    echo "✓ Created $dir"
done

# Create .gitkeep files to preserve empty directories
gitkeep_dirs=(
    "dags"
    "plugins"
    "src/spark_jobs/streaming"
    "src/spark_jobs/batch"
    "tests/unit"
    "tests/integration"
    "tests/e2e"
)

for dir in "${gitkeep_dirs[@]}"; do
    touch "$dir/.gitkeep"
    echo "✓ Added .gitkeep to $dir"
done

# Create __init__.py files for Python packages
python_packages=(
    "src"
    "src/data_generator"
    "src/spark_jobs"
    "src/kafka_setup"
    "src/monitoring"
)

for package in "${python_packages[@]}"; do
    touch "$package/__init__.py"
    echo "✓ Created __init__.py in $package"
done

# Create placeholder configuration files
touch config/kafka/server.properties
touch config/spark/spark-env.sh
touch config/airflow/airflow.cfg

# Create .dockerignore file
cat > .dockerignore << 'EOF'
# Python
__pycache__/
*.py[cod]
*$py.class
*.so
.Python
env/
venv/
ENV/
.pytest_cache/
.coverage
*.egg-info/

# Data files (these can be large)
data/raw/*
data/processed/*
data/lake/*
!data/raw/.gitkeep
!data/processed/.gitkeep

# Logs
logs/
*.log

# IDE
.idea/
.vscode/
*.swp
*.swo

# OS
.DS_Store
Thumbs.db

# Git
.git/
.gitignore

# Documentation
docs/
*.md

# Terraform state files
*.tfstate
*.tfstate.*
.terraform/

# Environment files with secrets
.env
.env.*
EOF

echo "✓ Created .dockerignore file"

# Create a comprehensive .gitignore if it doesn't exist
if [ ! -f .gitignore ]; then
    cat > .gitignore << 'EOF'
# Byte-compiled / optimized / DLL files
__pycache__/
*.py[cod]
*$py.class

# Virtual environments
venv/
env/
ENV/

# IDE specific files
.idea/
.vscode/
*.swp
*.swo

# OS specific files
.DS_Store
Thumbs.db

# Logs
logs/
*.log

# Data files
data/raw/*
data/processed/*
data/lake/*
!data/**/.gitkeep

# Environment variables
.env
.env.*

# Terraform
*.tfstate
*.tfstate.*
.terraform/

# Airflow
airflow.db
airflow-webserver.pid
standalone_admin_password.txt

# Jupyter Notebooks
.ipynb_checkpoints/

# Coverage reports
htmlcov/
.coverage
.coverage.*
coverage.xml
*.cover

# Distribution / packaging
.Python
build/
develop-eggs/
dist/
downloads/
eggs/
.eggs/
lib/
lib64/
parts/
sdist/
var/
wheels/
*.egg-info/
.installed.cfg
*.egg
EOF
    echo "✓ Created .gitignore file"
fi

echo ""
echo "📁 Project structure created successfully!"
echo ""
echo "Next steps:"
echo "1. Copy your existing files to their new locations:"
echo "   - Move docker-compose.yml to project root (if not already there)"
echo "   - Copy Dockerfiles to their respective docker/* subdirectories"
echo "   - Copy the requirements files to the project root"
echo ""
echo "2. Your working directory should be: $(pwd)"
echo ""
echo "3. To build and start services:"
echo "   docker-compose build"
echo "   docker-compose up -d"
echo ""