FROM --platform=linux/amd64 ubuntu:22.04

ENV SBT_VERSION 1.5.5

RUN apt-get update && apt-get install -y wget && apt-get install -y dpkg-dev && \ 
    apt-get install -y coreutils && apt-get install -y unzip && apt-get install -y curl && apt-get install -y git

RUN apt-get update && apt-get install -y openjdk-8-jdk

RUN apt-get remove scala-library scala && \
    wget https://downloads.lightbend.com/scala/2.13.8/scala-2.13.8.deb && \
    dpkg -i scala-2.13.8.deb && \
    apt-get update && \
    apt-get install -y scala

RUN echo "deb https://repo.scala-sbt.org/scalasbt/debian all main" | tee /etc/apt/sources.list.d/sbt.list && \
    echo "deb https://repo.scala-sbt.org/scalasbt/debian /" | tee /etc/apt/sources.list.d/sbt_old.list && \
    curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2EE0EA64E40A89B84B2DF73499E82A75642AC823" | apt-key add && \
    apt-get update && \
    apt-get install sbt

WORKDIR /home

RUN mkdir data
RUN mkdir output

WORKDIR /home

EXPOSE 8000
