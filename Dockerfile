# Minimal Docker image for building and running wing

FROM debian:11

RUN apt-get update && apt-get install -y \
  build-essential \
  g++ \
  zip unzip python3\
  cmake \
  git \
  make \
  sudo