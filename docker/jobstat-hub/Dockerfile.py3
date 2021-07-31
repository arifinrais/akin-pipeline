FROM python:3.9-slim
LABEL author="Arifin Rais"
LABEL affiliation="Institut Teknologi Bandung"

# os setup
RUN apt-get update
RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app

COPY src/jobstat-hub /usr/src/app
RUN pip install --no-cache-dir -r requirements.txt
COPY docker/jobstat-hub/config.py /usr/src/app/
