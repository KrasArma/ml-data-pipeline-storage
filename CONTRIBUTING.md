# Contributing to ML Data Pipeline Storage

Thank you for your interest in contributing to the ML Data Pipeline Storage project! This document provides guidelines and instructions for contributing to the project.

## Table of Contents

- [Code of Conduct](#code-of-conduct)
- [Getting Started](#getting-started)
- [Development Setup](#development-setup)
- [Project Structure](#project-structure)
- [Development Workflow](#development-workflow)
- [Coding Standards](#coding-standards)
- [Testing Guidelines](#testing-guidelines)
- [Documentation](#documentation)
- [Pull Request Process](#pull-request-process)
- [Issue Reporting](#issue-reporting)
- [TODO Management](#todo-management)

## Code of Conduct

This project follows a code of conduct that ensures a welcoming environment for all contributors. Please be respectful and constructive in all interactions.

## Getting Started

### Prerequisites

- Docker and Docker Compose
- Python 3.8+
- Git
- Basic understanding of machine learning pipelines
- Familiarity with microservices architecture

### Development Setup

1. **Fork and Clone the Repository**
   ```bash
   git clone https://github.com/yourusername/ml-data-pipeline-storage.git
   cd ml-data-pipeline-storage
   ```

2. **Create a Virtual Environment**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install Dependencies**
   ```bash
   pip install -r requirements-dev.txt
   ```

4. **Set Up Environment Variables**
   ```bash
   cp .env.example .env
   # Edit .env with your configuration
   ```

5. **Start Development Environment**
   ```bash
   docker-compose up -d
   ```

## Project Structure

```
ml-data-pipeline-storage/
├── airflow/                    # Apache Airflow configuration
│   ├── dags/                  # ETL pipeline definitions
│   ├── modules/               # Custom Airflow modules
│   ├── Dockerfile
│   └── requirements.txt
├── mlflow/                    # ML model management
│   ├── model/                 # Training scripts and data
│   ├── Dockerfile
│   └── requirements.txt
├── monitoring/                # Observability stack
│   ├── grafana/              # Dashboard configurations
│   └── prometheus.yml
├── service/                   # FastAPI prediction service
│   ├── app/                   # Application code
│   ├── tests/                 # Test suite
│   ├── Dockerfile
│   └── requirements.txt
├── docker-compose.yml
├── README.md
└── CONTRIBUTING.md
```

## Development Workflow

### 1. Create a Feature Branch

```bash
git checkout -b feature/your-feature-name
# or
git checkout -b bugfix/issue-description
# or
git checkout -b hotfix/critical-fix
```

### 2. Make Your Changes

- Follow the coding standards outlined below
- Write tests for new functionality
- Update documentation as needed
- Update TODO items as you work

### 3. Test Your Changes

```bash
# Run unit tests
pytest tests/unit/

# Run integration tests
pytest tests/integration/

# Run all tests with coverage
pytest --cov=app tests/

# Run linting
flake8 .
mypy .
```

### 4. Commit Your Changes

```bash
git add .
git commit -m "feat: add new feature description"
```

Use conventional commit messages:
- `feat:` for new features
- `fix:` for bug fixes
- `docs:` for documentation changes
- `test:` for test additions/changes
- `refactor:` for code refactoring
- `chore:` for maintenance tasks

### 5. Push and Create Pull Request

```bash
git push origin feature/your-feature-name
```

Then create a Pull Request on GitHub.

## Coding Standards

### Python Code

- Follow PEP 8 style guidelines
- Use type hints for function parameters and return values
- Write docstrings for all public functions and classes
- Use meaningful variable and function names
- Keep functions small and focused

### Example Code Style

```python
from typing import List, Dict, Optional
import logging

logger = logging.getLogger(__name__)

def process_data(
    data: List[Dict[str, any]], 
    config: Optional[Dict[str, any]] = None
) -> Dict[str, any]:
    """
    Process input data according to configuration.
    
    Args:
        data: List of data dictionaries to process
        config: Optional configuration dictionary
        
    Returns:
        Processed data dictionary
        
    Raises:
        ValueError: If data format is invalid
    """
    if not data:
        raise ValueError("Data cannot be empty")
    
    # Implementation here
    return processed_data
```

### Docker

- Use multi-stage builds when appropriate
- Keep images as small as possible
- Use specific version tags
- Follow security best practices

### Configuration

- Use environment variables for configuration
- Provide sensible defaults
- Document all configuration options
- Use configuration validation

## Testing Guidelines

### Unit Tests

- Test individual functions and methods
- Use pytest fixtures for test data
- Aim for high code coverage (>80%)
- Mock external dependencies

### Integration Tests

- Test component interactions
- Use test databases and services
- Test API endpoints
- Verify data flow through the pipeline

### Test Structure

```python
# tests/unit/test_data_processor.py
import pytest
from app.data_processor import DataProcessor

class TestDataProcessor:
    def test_process_valid_data(self):
        # Arrange
        processor = DataProcessor()
        test_data = [{"id": 1, "value": "test"}]
        
        # Act
        result = processor.process(test_data)
        
        # Assert
        assert result is not None
        assert len(result) == 1
```

## Documentation

### Code Documentation

- Write clear docstrings for all public APIs
- Include examples in docstrings when helpful
- Document complex algorithms and business logic
- Keep comments up to date with code changes

### API Documentation

- Document all API endpoints
- Include request/response examples
- Document error codes and messages
- Keep OpenAPI/Swagger specs updated

### README Updates

- Update README.md for significant changes
- Include setup instructions for new features
- Update configuration examples
- Document new environment variables

## Pull Request Process

### Before Submitting

1. Ensure all tests pass
2. Run linting and fix any issues
3. Update documentation
4. Update TODO items
5. Test your changes thoroughly

### PR Description Template

```markdown
## Description
Brief description of changes

## Type of Change
- [ ] Bug fix
- [ ] New feature
- [ ] Breaking change
- [ ] Documentation update

## Testing
- [ ] Unit tests added/updated
- [ ] Integration tests added/updated
- [ ] Manual testing completed

## Checklist
- [ ] Code follows project style guidelines
- [ ] Self-review completed
- [ ] Documentation updated
- [ ] TODO items updated
```

### Review Process

1. Automated checks must pass
2. At least one maintainer review required
3. Address all review comments
4. Keep PR focused and small when possible
5. Update PR description if scope changes

## Issue Reporting

### Bug Reports

Use the bug report template and include:
- Clear description of the issue
- Steps to reproduce
- Expected vs actual behavior
- Environment details
- Logs and error messages

### Feature Requests

Use the feature request template and include:
- Clear description of the feature
- Use case and motivation
- Proposed implementation approach
- Alternative solutions considered

## TODO Management

### Project TODO Structure

The project uses a structured TODO system in the main `TODO` file:

```
README:
    [ ] Setup instructions
    [ ] Configuration steps
    [ ] Usage examples

FEATURES:
    [ ] Core functionality items
    [ ] Integration features
    [ ] Performance improvements

ROADMAP:
    [ ] Long-term goals
    [ ] Major milestones
    [ ] Strategic initiatives

BACKLOG:
    [ ] Future considerations
    [ ] Nice-to-have features
    [ ] Research items
```

### TODO Guidelines

- Use clear, actionable descriptions
- Assign TODO items to specific contributors when possible
- Update TODO status as work progresses
- Move completed items to appropriate sections
- Use consistent formatting and indentation

### Adding TODO Items

When adding new TODO items:
1. Choose the appropriate section (README, FEATURES, ROADMAP, BACKLOG)
2. Use consistent checkbox format: `[ ] Description`
3. Be specific and actionable
4. Consider priority and dependencies

## Getting Help

- Check existing issues and discussions
- Ask questions in GitHub Discussions
- Contact maintainers directly for urgent issues
- Review documentation and code comments

## Recognition

Contributors will be recognized in:
- CONTRIBUTORS.md file
- Release notes
- Project documentation
- GitHub contributor graphs

Thank you for contributing to the ML Data Pipeline Storage project!
