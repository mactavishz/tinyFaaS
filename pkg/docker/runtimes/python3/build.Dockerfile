ARG PYTHON_VERSION=3.14.1
ARG ALPINE_VERSION=3.23

FROM python:${PYTHON_VERSION}-alpine${ALPINE_VERSION}

# Create app directory
WORKDIR /usr/src/app

COPY main.py .
