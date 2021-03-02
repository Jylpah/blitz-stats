# BlitzAnalysiz Dockerfile
FROM python:slim-buster

ENV PATH /app/getBlitzStats:/usr/local/bin:$PATH
# extra dependencies (over what buildpack-deps already includes)
WORKDIR /app/getBlitzStats
RUN mkdir data
COPY requirements.txt ./
RUN python3.9 -m pip install --no-cache-dir -U pip
RUN python3.9 -m pip install --no-cache-dir -r requirements.txt
COPY tanks.json maps.json ./
COPY *.py ./
CMD ["/bin/bash"]
