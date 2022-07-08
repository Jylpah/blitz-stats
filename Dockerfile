# blitz-stats Dockerfile
FROM python:3.10-slim-buster

ENV PATH /app/getBlitzStats:/usr/local/bin:$PATH
# extra dependencies (over what buildpack-deps already includes)
WORKDIR /app/getBlitzStats
RUN mkdir data
COPY requirements.txt ./
RUN python3.10 -m pip install --no-cache-dir -U pip
RUN python3.10 -m pip install --no-cache-dir -r requirements.txt
COPY tanks.json maps.json ./
COPY *.py ./
COPY blitzstats.ini.default ./
CMD ["/bin/bash"]
