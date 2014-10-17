/* Copyright (c) 2014 Tim Berning */

#ifndef CHUNK_H_
#define CHUNK_H_

#include <stdint.h>

void* initalize_nvm_space(const char *workspace_path, uint64_t max_num_chunks);

uint64_t recover_chunks();

void* activate_more_chunks(uint64_t n_chunks);

void cleanup_chunks();

#endif /* CHUNK_H_ */