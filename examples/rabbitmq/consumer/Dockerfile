FROM python:3.10-slim-buster

RUN pip install --upgrade pip

WORKDIR /consumer-app

COPY requirements.txt requirements.txt
COPY consumer.py consumer.py

RUN pip install --user -r requirements.txt

CMD ["python", "consumer.py"]
