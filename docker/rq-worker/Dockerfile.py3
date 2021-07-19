FROM python:3.9-slim
LABEL author="Arifin Rais"
LABEL affiliation="Institut Teknologi Bandung"

# os setup
RUN apt-get update
RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app

# install requirements
COPY docker/rq-worker/requirements.txt /usr/src/app/
RUN pip install --no-cache-dir -r requirements.txt

# run command
CMD ["rq", "worker"]