# Stage: Generator
FROM --platform=linux/amd64 ubuntu:22.04 AS generator

WORKDIR /script

COPY docker/worker/script .
RUN ls -al

RUN ./generator.sh


# Stage: Runner
FROM sbtscala/scala-sbt:openjdk-17.0.2_1.8.0_2.13.10

WORKDIR /code

COPY . .

RUN mkdir input-data
RUN mkdir output-data

#COPY --from=generator /script/partition* input-data

ENV SBT_OPTS="-Xmx8G -Xms4G -Xss1G"

ENTRYPOINT ["sbt"]
