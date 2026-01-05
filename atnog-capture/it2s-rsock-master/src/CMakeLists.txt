INCLUDE_DIRECTORIES(../include)
ADD_LIBRARY(it2s-rsock rsock.c utils.c)

INSTALL(FILES
    ../include/it2s-rsock/rsock.h
    ../include/it2s-rsock/utils.h
    DESTINATION include/it2s-rsock
)

INSTALL(
    TARGETS
    it2s-rsock
    LIBRARY DESTINATION lib
    COMPONENT lib 
)
