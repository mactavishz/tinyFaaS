FROM tinyfaas-runtime-nodejs

EXPOSE 8000

WORKDIR /usr/src/app

COPY fn fn

RUN npm install ./fn

CMD [ "node", "index.js" ]
