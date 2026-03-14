cmake_minimum_required(VERSION 3.16)

add_executable(its_sniffer
    its_sniffer.c
    logger.c
)

add_executable(its_producer
    its_producer.c
    logger.c
)

target_link_libraries(its_sniffer
    -lpthread
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
    -lpthread
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
