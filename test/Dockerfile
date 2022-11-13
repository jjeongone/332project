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


RUN curl -L -o sbt-$SBT_VERSION.zip https://github.com/sbt/sbt/releases/download/v$SBT_VERSION/sbt-$SBT_VERSION.zip && \
    unzip sbt-$SBT_VERSION.zip -d ops

WORKDIR /home

RUN mkdir gensort
RUN mkdir data

COPY gensort-linux-1.5.tar /home/gensort

WORKDIR /home/gensort

RUN tar -xvf gensort-linux-1.5.tar

WORKDIR /home/gensort/64

RUN ./gensort -a 330000 partition1
RUN ./gensort -a 330000 partition2
RUN mv partition1 partition2 /home/data

WORKDIR /home

EXPOSE 8000
