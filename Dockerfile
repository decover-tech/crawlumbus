# syntax=docker/dockerfile:1
FROM python:3.8-slim-buster
RUN apt-get update && \
    apt-get install -y git gcc curl wget build-essential && \
    apt-get --reinstall install -y build-essential
COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt
COPY .. .
EXPOSE 80
CMD ["python3", "-m", "main"]