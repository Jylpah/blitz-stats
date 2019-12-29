# BlitzAnalysiz Dockerfile
FROM ubuntu
ENV PATH /app/getBlitzStats:/usr/local/bin:$PATH
# extra dependencies (over what buildpack-deps already includes)
RUN apt-get update && apt-get install -y --no-install-recommends python3.8 python3-pip
WORKDIR /app/getBlitzStats
COPY requirements.txt ./
RUN python3.8 -m pip install --no-cache-dir -U pip
RUN python3.8 -m pip install --no-cache-dir -r requirements.txt 
COPY *.py ./