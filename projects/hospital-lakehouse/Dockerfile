FROM quay.io/astronomer/astro-runtime:9.4.0

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-11-jre-headless && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-arm64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER astro