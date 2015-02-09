/* Copyright (c) 2014 Tim Berning */

#ifndef UTIL_H_
#define UTIL_H_

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

inline uint64_t round_up(uint64_t num, uint64_t multiple);
inline char identify_usage(void *ptr);

inline void clflush(const void *ptr);
inline void clflush_range(const void *ptr, uint64_t len);

#ifdef HAS_CLFLUSHOPT
inline void clflushopt(const void *ptr);
inline void clflushopt_range(const void *ptr, uint64_t len);
#endif

#ifdef HAS_CLWB
inline void clwb(const void *ptr);
inline void clwb_range(const void *ptr, uint64_t len);
#endif

inline void sfence();
inline void mfence();

/* macros for persistency depending on instruction availability */
#ifdef HAS_CLWB
    /* CLWB is the preferred instruction, not invalidating any cache lines */
    #define PERSIST(ptr) sfence(); clwb(ptr); sfence()
    #define PERSIST_RANGE(ptr, len) sfence(); clwb_range(ptr, len); sfence()
#elif HAS_CLFLUSHOPT
    /* CLFLUSHOPT is preferred over CLFLUSH as only dirty cache lines will be evicted */
    #define PERSIST(ptr) sfence(); clflushopt(ptr); sfence()
    #define PERSIST_RANGE(ptr, len) sfence(); clflushopt_range(ptr, len); sfence()
#else
    /* If neither CLWB nor CLFLUSHOPT are available, default to CLFLUSH */
    #define PERSIST(ptr) mfence(); clflush(ptr); mfence()
    #define PERSIST_RANGE(ptr, len) mfence(); clflush_range(ptr, len); mfence()
#endif

#endif /* UTIL_H_ */
