#https://nodejs.org/en/docs/guides/nodejs-docker-webapp/
ARG NODE_VERSION=24.13
ARG ALPINE_VERSION=3.23

FROM node:${NODE_VERSION}-alpine${ALPINE_VERSION}

# Create app directory
WORKDIR /usr/src/app

RUN npm install -g pnpm@10.28.2

COPY package.json .

RUN pnpm install --prod

COPY index.js .
