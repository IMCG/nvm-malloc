/* Copyright (c) 2014 Tim Berning */

#include "util.h"

#include "types.h"

extern void *nvm_start;

uint64_t round_up(uint64_t num, uint64_t multiple) {
    uint64_t rest = 0;
    if (multiple == 0)
        return num;
    rest = num % multiple;
    if (rest == 0)
        return num;
    return num + multiple - rest;
}

char identify_usage(void *ptr) {
    /* find out if ptr points to a small, large or huge region */
    nvm_block_header_t *nvm_block = NULL;
    void *rel_ptr = (void*) ((uintptr_t)ptr - (uintptr_t)nvm_start);
    if (rel_ptr % CHUNK_SIZE == sizeof(nvm_huge_header_t)) {
        /* ptr is 64 bytes into a chunk, must be huge allocation */
        return USAGE_HUGE;
    } else if (rel_ptr % BLOCK_SIZE > sizeof(nvm_block_header_t)) {
        /* ptr is more than 64 bytes into a block, must be a small allocation */
        return USAGE_RUN;
    } else if (rel_ptr % BLOCK_SIZE == sizeof(nvm_block_header_t)) {
        /* ptr is exactly 64 bytes into a block, can be either small or large --> now we must check header */
        nvm_block = (nvm_block_header_t*) ((uintptr_t)ptr - sizeof(nvm_block_header_t));
        if (GET_USAGE(nvm_block->state) == USAGE_BLOCK || GET_USAGE(nvm_block->state) == USAGE_FREE) {
            return USAGE_BLOCK;
        } else {
            return USAGE_RUN;
        }
    } else {
        /* if we get here something went wrong... */
        return (char)-1;
    }
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
