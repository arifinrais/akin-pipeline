FROM python:3.9
MAINTAINER Madison Bahmer <madison.bahmer@istresearch.com>

# os setup
RUN apt-get update
RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app

# install requirements
COPY src/ingestion-monitor/requirements.txt /usr/src/app/
RUN pip install --no-cache-dir -r requirements.txt

# move codebase over
COPY src/ingestion-monitor /usr/src/app

# override settings via localsettings.py
COPY src/ingestion-monitor/config.py /usr/src/app/config.py

# copy testing script into container
# COPY docker/run_docker_tests.sh /usr/src/app/run_docker_tests.sh

# set up environment variables

# run command
CMD ["python", "ingestion_monitor.py", "run"]