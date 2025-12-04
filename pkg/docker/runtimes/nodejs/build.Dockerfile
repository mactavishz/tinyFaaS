#https://nodejs.org/en/docs/guides/nodejs-docker-webapp/
ARG NODE_VERSION=24.11.1
ARG ALPINE_VERSION=3.23

FROM node:${NODE_VERSION}-alpine${ALPINE_VERSION}

# Create app directory
WORKDIR /usr/src/app

COPY index.js .
COPY package.json .

RUN npm install express@5 && \
    npm cache clean --force
