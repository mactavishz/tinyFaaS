ARG PYTHON_VERSION=3.14

FROM python:${PYTHON_VERSION}-slim-bookworm

# Create app directory
WORKDIR /usr/src/app

COPY main.py .
