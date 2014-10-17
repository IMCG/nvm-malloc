/* Copyright (c) 2014 Tim Berning */

#include "util.h"

uint64_t round_up(uint64_t num, uint64_t multiple) {
    uint64_t rest = 0;
    if (multiple == 0)
        return num;
    rest = num % multiple;
    if (rest == 0)
        return num;
    return num + multiple - rest;
}

void clflush(const void *ptr) {
    asm volatile("clflush %0" : "+m" (ptr));
}

void clflush_range(const void *ptr, uint64_t len) {
    uintptr_t start = (uintptr_t)ptr & ~(0x63);
    for (; start < (uintptr_t)ptr + len; start += 64) {
        clflush((void*)start);
    }
}

void sfence() {
    asm volatile("sfence":::"memory");
}