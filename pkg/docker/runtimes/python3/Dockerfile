FROM tinyfaas-runtime-python3

EXPOSE 8000

WORKDIR /usr/src/app

COPY fn/* ./
RUN touch requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

ENV PYTHONUNBUFFERED=1
CMD ["fastapi", "run", "main.py", "--port", "8000"]
