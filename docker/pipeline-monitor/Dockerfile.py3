FROM python:3.9-slim
LABEL author="Arifin Rais"
LABEL affiliation="Institut Teknologi Bandung"

# os setup
RUN apt-get update
RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app

# install requirements
#COPY src/utils /usr/src/utils
COPY src/pipeline-monitor/requirements.txt /usr/src/app/
RUN pip install --no-cache-dir -r requirements.txt
#RUN rm -rf /usr/src/utils

ENV FLASK_RUN_PORT=5050
# move codebase over
COPY src/pipeline-monitor /usr/src/app

# override settings via localsettings.py
COPY docker/pipeline-monitor/config.py /usr/src/app/config.py

# copy testing script into container
# COPY docker/run_docker_tests.sh /usr/src/app/run_docker_tests.sh

# set up environment variables

# run command
CMD [ "python3", "-m" , "flask", "run", "--host=0.0.0.0"]