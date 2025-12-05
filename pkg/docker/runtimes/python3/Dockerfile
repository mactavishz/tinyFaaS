FROM tinyfaas-runtime-python3

EXPOSE 8000

WORKDIR /usr/src/app

COPY fn/* ./
RUN python -m pip install -r requirements.txt --user

ENV PYTHONUNBUFFERED=1
CMD [ "python3", "main.py" ]
