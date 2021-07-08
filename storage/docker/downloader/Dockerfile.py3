FROM python:3.6

# os setup
RUN apt-get update
RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app

# install requirements
COPY utils /usr/src/utils
COPY downloader/requirements.txt /usr/src/app/
RUN pip install --no-cache-dir -r requirements.txt
RUN rm -rf /usr/src/utils

# move codebase over
COPY downloader /usr/src/app

# override settings via localsettings.py
COPY docker/downloader/settings.py /usr/src/app/localsettings.py

# set up environment variables

# run command
CMD ["python", "downloader.py", "dump", "-t", "demo.crawled_firehose"]