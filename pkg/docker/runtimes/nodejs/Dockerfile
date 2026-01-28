FROM tinyfaas-runtime-nodejs

EXPOSE 8000

WORKDIR /usr/src/app

COPY fn/package.json fn/package.json

RUN pnpm add ./fn

COPY fn fn

CMD [ "node", "index.js" ]
