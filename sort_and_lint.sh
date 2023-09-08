#!/bin/bash
# Perform a little cleaning on our imports.
echo "Using isort utility..."
isort .
# Lint our python scripts ignoring the contested line length (PEP8 E501).
echo "Linting with flake8..."
flake8 --ignore=E501 --exclude=__pycache__,sort_and_lint.sh,data,.git,.gitignore,Dockerfile,README.md,.idea,*.feature,*.txt,*.json,*.sh raritan/*.py
flake8 --ignore=E501 --exclude=__pycache__,sort_and_lint.sh,data,.git,.gitignore,Dockerfile,README.md,.idea,*.feature,*.txt,*.json,*.sh example/*.py
