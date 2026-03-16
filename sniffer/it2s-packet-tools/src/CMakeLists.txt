cmake_minimum_required(VERSION 3.16)

find_package(Threads REQUIRED)
find_package(CURL REQUIRED)

add_executable(its_sniffer
    its_sniffer.c
    logger.c
    cloudevent_sender.c
)

add_executable(its_producer
    its_producer.c
    logger.c
)

target_link_libraries(its_sniffer
    PRIVATE
    Threads::Threads
    CURL::libcurl
    -lit2s-rsock
    -lit2s-ublox
    -lit2s-mac
    -lit2s-llc
    -lit2s-gn
    -lit2s-asn-etsi-its-v2-cdd-2.2.1
    -lit2s-asn-etsi-its-v2-cam
    -lit2s-asn-etsi-its-v2-denm
)

target_link_libraries(its_producer
    PRIVATE
    Threads::Threads
    -lit2s-rsock
    -lit2s-ublox
    -lit2s-mac
    -lit2s-llc
    -lit2s-gn
    -lit2s-asn-etsi-its-v2-cdd-2.2.1
    -lit2s-asn-etsi-its-v2-cam
    -lit2s-asn-etsi-its-v2-denm
)

install(TARGETS its_producer its_sniffer RUNTIME DESTINATION bin)
