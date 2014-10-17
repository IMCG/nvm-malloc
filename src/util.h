/* Copyright (c) 2014 Tim Berning */

#ifndef UTIL_H_
#define UTIL_H_

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

inline uint64_t round_up(uint64_t num, uint64_t multiple);
inline void clflush(const void *ptr);
inline void clflush_range(const void *ptr, uint64_t len);
inline void sfence();

#define DEBUG(fmt, args...) printf(fmt, args)

#endif /* UTIL_H_ */