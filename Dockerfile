FROM python:3.7.9
ENV PATH /usr/local/bin:$PATH
ADD . /code
USER root
WORKDIR /code
RUN apt-get update \
    && apt-get -y install libc-dev \
    && apt-get -y install build-essential \
    && pip install -U pip \
    && pip install -r requirements.txt \
    && pip install numpy --index-url=https://www.piwheels.org/simple/
# CMD python api.py