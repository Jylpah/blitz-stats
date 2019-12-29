# BlitzAnalysiz Dockerfile
#FROM python:slim-buster
FROM python:alpine
#FROM ubuntu

ENV PATH /app/getBlitzStats:/usr/local/bin:$PATH
# extra dependencies (over what buildpack-deps already includes)
#RUN apt-get update && apt-get install -y python3.8 python3-pip
RUN apk update && apk add libxml2-dev libxslt-dev
WORKDIR /app/getBlitzStats
#RUN python3.8 -m pip install --no-cache-dir -U pip
RUN pip install --no-cache-dir -U pip
COPY requirements.txt ./
#RUN python3.8 -m pip install --no-cache-dir -r requirements.txt 
RUN pip install --no-cache-dir -r requirements.txt 
COPY *.py ./