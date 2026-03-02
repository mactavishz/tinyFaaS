#https://nodejs.org/en/docs/guides/nodejs-docker-webapp/
ARG NODE_VERSION=24

FROM node:${NODE_VERSION}-bookworm-slim

# Create app directory
WORKDIR /usr/src/app

RUN npm install -g pnpm@10.28.2

COPY package.json .

RUN pnpm install --prod

COPY index.js .
