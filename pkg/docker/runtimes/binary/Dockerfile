FROM tinyfaas-runtime-binary

EXPOSE 8000

WORKDIR /usr/src/app

COPY fn/* ./
RUN chmod +x handler.sh

CMD [ "./runtime.bin" ]
