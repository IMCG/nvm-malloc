/* Copyright (c) 2014 Tim Berning */

#include "chunk.h"

#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdarg.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <stddef.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/mman.h>
#include <unistd.h>

#include "types.h"

#ifdef __APPLE__
#define MAP_ANONYMOUS MAP_ANON
#define posix_fallocate(...) 0
#endif

inline void error_and_exit(char *msg, ...) {
    va_list args;
    va_start(args, msg);
    vfprintf(stderr, msg, args);
    va_end(args);
    exit(-1);
}

static void            *chunk_region_start = NULL;
static uint64_t        num_chunks = 0;
static int             *chunk_fds = NULL;
static uint64_t        next_unmapped_chunk = 0;
static char            *next_file_path = NULL;
static int32_t         base_path_length = 0;
static pthread_mutex_t chunk_mtx = PTHREAD_MUTEX_INITIALIZER;

int open_existing_file(char *path) {
    int fd=-1;
    struct stat stbuf;
    if (stat(path, &stbuf) < 0) {
        return -1;
    } else if ((fd = open(path, O_RDWR)) < 0) {
        error_and_exit("unable to open file %s\n", path);
    }
    return fd;
}

int open_empty_or_create_file(char *path) {
    int fd=-1;
    struct stat stbuf;
    if (stat(path, &stbuf) < 0) {
        if ((fd = open(path, O_RDWR|O_CREAT, 0666)) < 0)
            error_and_exit("unable to create file %s\n", path);
    } else if ((fd = open(path, O_RDWR|O_TRUNC)) < 0) {
            error_and_exit("unable to open file %s\n", path);
    }
    return fd;
}

void* initalize_nvm_space(const char *workspace_path, uint64_t max_num_chunks) {
    void *mem = NULL;
    uint64_t i = 0;

    // just to be safe
    chunk_region_start = NULL;
    num_chunks = max_num_chunks;
    chunk_fds = NULL;
    next_unmapped_chunk = 0;
    next_file_path = NULL;
    base_path_length = 0;

    // reserve space for file descriptors
    chunk_fds = (int*) malloc(num_chunks * sizeof(int));
    for (i = 0; i < num_chunks; ++i)
        chunk_fds[i] = -1;

    // perform initial request for large memory block, CHUNK_SIZE *must* be a multiple of 2mb
    if ((mem = mmap(NULL, num_chunks * CHUNK_SIZE, PROT_NONE, MAP_SHARED | MAP_ANONYMOUS | MAP_NORESERVE, -1, 0)) == MAP_FAILED)
        error_and_exit("Unable to mmap initial block of %lu chunks\n", num_chunks);
    chunk_region_start = mem;

    i = strlen(workspace_path);
    next_file_path = (char*) malloc(i + 1 + 3 + 8 + 1); /* <workspace_path> + '/' + 'map' + '00000000' + '\0' */
    strncpy(next_file_path, workspace_path, i);
    sprintf(&next_file_path[i], "/map000000000"); /* '\0' is inserted automatically */
    base_path_length = i+4;

    return chunk_region_start;
}

uint64_t recover_chunks() {
    int i=0, fd=-1;
    void *next_chunk_addr = (void*) ((char*)chunk_region_start + next_unmapped_chunk * CHUNK_SIZE);

    while ((fd = open_existing_file(next_file_path)) >= 0) {
        /*if (munmap(next_chunk_addr, CHUNK_SIZE) != 0)
            error_and_exit("unable to munmap %p", next_chunk_addr);*/
        if (mmap(next_chunk_addr, CHUNK_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_NORESERVE | MAP_FIXED, fd, 0) == MAP_FAILED)
            error_and_exit("error mapping chunk %lu to file %s", i, next_file_path);

        chunk_fds[next_unmapped_chunk] = fd;

        next_chunk_addr = (void*) ((char*)next_chunk_addr + CHUNK_SIZE);
        sprintf(&next_file_path[base_path_length], "%09lu", ++next_unmapped_chunk);
    }

    return next_unmapped_chunk;
}

void* activate_more_chunks(uint64_t n_chunks) {
    int i=0, fd=-1;
    uint64_t first_chunk_id;
    void *next_chunk_addr=NULL, *result=NULL;
    char file_path[base_path_length+9];
    strncpy(file_path, next_file_path, base_path_length);

    pthread_mutex_lock(&chunk_mtx);
    first_chunk_id = __sync_fetch_and_add(&next_unmapped_chunk, n_chunks);

    next_chunk_addr = (void*) ((uintptr_t)chunk_region_start + first_chunk_id*CHUNK_SIZE);
    result = next_chunk_addr;

    if (n_chunks > num_chunks - first_chunk_id)
        error_and_exit("Requested too many chunks\n");

    for (i=0; i<n_chunks; ++i) {
        sprintf(&file_path[base_path_length], "%09lu", first_chunk_id);

        if ((fd = open_empty_or_create_file(file_path)) < 0)
            error_and_exit("unable to open or create file %s", file_path);
        /* >>>> HACK begin: call posix_fallocate with 1MB first to prevent PMFS from switching to huge pages */
        if (posix_fallocate(fd, 0, 1024*1024) != 0)
            error_and_exit("unable to ensure file size of %s", file_path);
        /* <<<< HACK end */
        if (posix_fallocate(fd, 0, CHUNK_SIZE) != 0)
            error_and_exit("unable to ensure file size of %s", file_path);
        if (mmap(next_chunk_addr, CHUNK_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_NORESERVE | MAP_FIXED, fd, 0) == MAP_FAILED)
            error_and_exit("error mapping chunk %lu to file %s", i, file_path);
        chunk_fds[first_chunk_id] = fd;

        next_chunk_addr = (void*) ((char*)next_chunk_addr + CHUNK_SIZE);
        ++first_chunk_id;
    }

    pthread_mutex_unlock(&chunk_mtx);

    return result;
}

void cleanup_chunks() {
    int i=0;
    void *next_chunk=chunk_region_start;
    for (i=0; i<num_chunks; ++i) {
        munmap(next_chunk, CHUNK_SIZE);
        if (chunk_fds[i] >= 0) {
            close(chunk_fds[i]);
            chunk_fds[i] = -1;
        }
    }
    chunk_region_start = NULL;
    num_chunks = 0;
    free(chunk_fds);
    chunk_fds = NULL;
    next_unmapped_chunk = 0;
    free(next_file_path);
    next_file_path = NULL;
    base_path_length = 0;
}