FROM ubuntu:noble

ARG ES_PUB_KEY=[REDACTED]
ARG TARGETPLATFORM=amd64
ARG APP=its_sniffer rsock lo
ENV APP=${APP}

RUN apt update && \
    apt install -y --no-install-recommends gnupg ca-certificates && \
    gpg --keyserver hkps://keyserver.ubuntu.com --recv-keys $ES_PUB_KEY && \
    gpg --export $ES_PUB_KEY | gpg --dearmor -o /usr/share/keyrings/es-key.gpg && \
    rm -rf /root/.gnupg
    
RUN if [ "$TARGETPLATFORM" = "amd64" ]; then \
        echo "deb [signed-by=/usr/share/keyrings/es-key.gpg arch=amd64] https://es.av.it.pt/repo/ubuntu/noble stable main" > /etc/apt/sources.list.d/es.list \
    ; elif [ "$TARGETPLATFORM" = "arm64" ]; then \
        echo "deb [signed-by=/usr/share/keyrings/es-key.gpg arch=arm64] https://es.av.it.pt/repo/ubuntu/noble stable main" > /etc/apt/sources.list.d/es.list \
    ; fi

RUN apt update && \
    apt install -y build-essential cmake cw-mk6-firmware-git it2s-rsock-git it2s-ublox-git it2s-mac-git it2s-llc-git it2s-gn-git it2s-asn-itss-etsi-git

WORKDIR /app
COPY . .

RUN rm -rf build && \
    mkdir -p build && \
    cd build && \
    cmake .. && \
    make install

ENTRYPOINT ["sh", "-c", "exec $APP"]
