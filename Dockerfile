# blitz-stats Dockerfile

FROM python:3.13-slim

LABEL maintainer="Jylpah@gmail.com"
LABEL description="Docker image for Blitz-stats, a stats collector for World of Tanks Blitz"

# Create a new group
RUN groupadd -g 1001 blitz && useradd -ms /bin/bash -u 1001 -g blitz blitzuser

# Install necessary packages, etc.
RUN apt-get update && apt-get upgrade -y 
RUN apt-get install -y git && \
    rm -Rf /var/lib/apt/lists/*

USER blitzuser:blitz

ENV PATH="/home/blitzuser/.local/bin:/app/blitz-stats:${PATH}"
# extra dependencies (over what buildpack-deps already includes)
WORKDIR /app/blitz-stats
RUN mkdir data
# COPY requirements.txt ./

COPY --chown=blitzuser:blitz tanks.json maps.json src tests pyproject.toml README.md ./
COPY --chown=blitzuser:blitz blitzstats.ini.docker ./blitzstats.ini

RUN python3.13 -m pip install --user --no-cache-dir -U pip
RUN python3.13 -m pip install --user --no-cache-dir .

CMD ["/bin/bash"]
