ARG PYTHON_VERSION=3.14

FROM python:${PYTHON_VERSION}-slim-bookworm

# Create app directory
WORKDIR /usr/src/app

RUN pip install --no-cache-dir --upgrade pip && pip install --no-cache-dir flask==3.1.3 waitress==3.0.2

COPY main.py .
