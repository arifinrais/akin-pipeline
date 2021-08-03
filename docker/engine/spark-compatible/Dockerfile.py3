FROM python:3.6-slim
LABEL author="Arifin Rais"
LABEL affiliation="Institut Teknologi Bandung"

# 
# os setup
RUN apt-get update
RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app

# install requirements
#COPY src/utils /usr/src/utils
COPY docker/engine/spark-compatible/requirements.txt /usr/src/app
RUN pip install --no-cache-dir -r requirements.txt
#RUN rm -rf /usr/src/utils

# move codebase over
COPY src/engine /usr/src/app/engine
COPY docker/engine/config.py /usr/src/app/engine
COPY src/engine/main.py /usr/src/app