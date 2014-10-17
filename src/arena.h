/* Copyright (c) 2014 Tim Berning */

#ifndef ARENA_H_
#define ARENA_H_

#include "types.h"

void arena_init(arena_t *arena, uint32_t id, nvm_chunk_header_t *first_chunk, int create_initial_block);

void arena_recover(arena_t *arena, uint32_t id, nvm_chunk_header_t *first_chunk);

void* arena_allocate(arena_t *arena, uint32_t n_bytes);

void arena_free(void *ptr, void **link_ptr1, void *target1, void **link_ptr2, void *target2);

#endif /* ARENA_H_ */