FROM python:3.9-slim
LABEL author="Arifin Rais"
LABEL affiliation="Institut Teknologi Bandung"

# os setup
RUN apt-get update
RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app

# install requirements
#COPY src/utils /usr/src/utils
COPY src/engine/requirements.txt /usr/src/app
RUN pip install --no-cache-dir -r requirements.txt --default-timeout=100
#RUN rm -rf /usr/src/utils

# move codebase over
COPY src/engine /usr/src/app/engine
COPY kubernetes/engine/config.py /usr/src/app/engine
COPY src/engine/main.py /usr/src/app
COPY res /usr/src/app/res

# override settings via config.py
# COPY src/engine/localsettings.py /usr/src/app/localsettings.py

# copy testing script into container
#COPY kubernetes/run_kubernetes_tests.sh /usr/src/app/run_kubernetes_tests.sh

# set up environment variables

# run command