From python:3.10

WORKDIR /app

COPY . .

RUN pip install -r requirements.txt

CMD ["sh", "-c", "echo sleeping 120s && sleep 120 && python csvProducer.py"]