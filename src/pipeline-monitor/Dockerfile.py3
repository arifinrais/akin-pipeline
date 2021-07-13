FROM python:3.9
MAINTAINER Madison Bahmer <madison.bahmer@istresearch.com>

# os setup
RUN apt-get update
RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app

# install requirements
COPY src/pipeline-monitor/requirements.txt /usr/src/app/
RUN pip install --no-cache-dir -r requirements.txt

# move codebase over
COPY src/pipeline-monitor /usr/src/app

# override settings via localsettings.py
COPY src/pipeline-monitor/config.py /usr/src/app/config.py

# copy testing script into container
# COPY docker/run_docker_tests.sh /usr/src/app/run_docker_tests.sh

# set up environment variables

# run command
CMD ["python", "pipeline_monitor.py", "run"]