ARG PYTHON_VERSION=3.14

FROM python:${PYTHON_VERSION}-slim-bookworm

# Create app directory
WORKDIR /usr/src/app

RUN pip install --no-cache-dir --upgrade pip && pip install --no-cache-dir fastapi[standard]==0.135.3

COPY main.py .
