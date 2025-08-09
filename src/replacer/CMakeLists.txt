set(LRU_SOURCES lru_replacer.cpp)
add_library(lru_replacer STATIC ${LRU_SOURCES})

set(CLOCK_SOURCES clock_replacer.cpp)
add_library(clock_replacer STATIC ${CLOCK_SOURCES})

set(LFU_SOURCES lfu_replacer.cpp)
add_library(lfu_replacer STATIC ${LFU_SOURCES})
