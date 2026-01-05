FROM alpine:3.20 AS build
RUN apk add --no-cache build-base linux-headers
WORKDIR /src

COPY src/rsock.c src/utils.c test/raw_test.c include/it2s-rsock/rsock.h include/it2s-rsock/utils.h ./
RUN mkdir -p it2s-rsock && cp rsock.h utils.h it2s-rsock/

RUN mkdir -p /out && gcc -O2 -Wall -Wextra -I. raw_test.c rsock.c utils.c -o /out/raw_test

FROM alpine:3.20
COPY --from=build /out/raw_test /usr/local/bin/raw_test
ENTRYPOINT ["/usr/local/bin/raw_test"]
