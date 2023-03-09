FROM python:3.10-slim-buster

RUN pip install --upgrade pip

WORKDIR /producer-app

COPY requirements.txt requirements.txt
COPY producer.py producer.py

RUN pip install --user -r requirements.txt

CMD ["python", "producer.py"]
