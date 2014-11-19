/* Copyright (c) 2014 Tim Berning */

#include "nvm_malloc.h"

#include <assert.h>
#include <pthread.h>
#include <string.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <sys/mman.h>
#include <unistd.h>

#include "arena.h"
#include "chunk.h"
#include "object_table.h"
#include "util.h"

#include <ulib/hash_chain_prot.h>
#include <ulib/util_algo.h>

void nvm_initialize_empty();
void nvm_initialize_recovered(uint64_t n_chunks_recovered);
nvm_huge_header_t* nvm_reserve_huge(uint64_t n_chunks);

/* comparison function for a the free chunk tree - sort by number of chunks */
int chunk_node_compare(const void *_a, const void *_b) {
    const huge_t *a = tree_entry(_a, huge_t, link);
    const huge_t *b = tree_entry(_b, huge_t, link);
    return generic_compare((uintptr_t)a->n_chunks, (uintptr_t)b->n_chunks);
}

static huge_t* tree_upper_bound(uint32_t req_chunks, struct tree_root *root) {
    huge_t *entry;
    huge_t *last_larger = NULL;
    while (root) {
        entry = tree_entry(root, huge_t, link);
        if (entry->n_chunks == req_chunks) {
            return entry;
        } else if (entry->n_chunks < req_chunks){
            root = root->right;
        } else {
            last_larger = entry;
            root = root->left;
        }
    }
    return last_larger;
}

/* start of mapped NVM space */
void *nvm_start = NULL;

/* global free chunk tree */
node_t *free_chunks = NULL;
pthread_mutex_t chunk_mtx = PTHREAD_MUTEX_INITIALIZER;

/* thread -> arena mapping as hash table */
DEFINE_CHAINHASH(tmap,
                 pid_t,
                 arena_t*,
                 1,
                 chainhash_hashfn,
                 chainhash_equalfn,
                 chainhash_cmpfn);
arena_t **arenas=NULL;
static uint32_t next_arena=0;
static chainhash_t(tmap) *tidmap = NULL;

void* nvm_initialize(const char *workspace_path, int recover_if_possible) {
    uint64_t n_chunks_recovered = 0;

    assert(nvm_start == NULL);
    nvm_start = initalize_nvm_space(workspace_path, MAX_NVM_CHUNKS);

    tidmap = chainhash_init(tmap, INITIAL_ARENAS);

    if (!recover_if_possible || (n_chunks_recovered = recover_chunks()) == 0) {
        /* no chunks were recovered, this is a fresh start so initialize */
        nvm_initialize_empty();
        ot_init(nvm_start);
    } else {
        /* chunks were recovered, perform cleanup and consistency check */
        nvm_initialize_recovered(n_chunks_recovered);
        ot_init(nvm_start);
        ot_recover();
    }

    return nvm_start;
}

void* nvm_reserve(uint64_t n_bytes) {
    void *mem = NULL;
    arena_t *arena = NULL;
    huge_t *huge = NULL;
    nvm_huge_header_t *nvm_huge=NULL;
    pid_t tid;
    uint64_t n_chunks, next_arena_num;

    if (n_bytes <= SCLASS_LARGE_MAX) {
        /* determine arena for calling thread */
        tid = (uint64_t) syscall(SYS_gettid);
        chainhash_itr_t(tmap) it = chainhash_get(tmap, tidmap, tid);
        if (chainhash_end(it)) {
            next_arena_num = __sync_fetch_and_add(&next_arena, 1) % INITIAL_ARENAS;
            arena = arenas[next_arena_num];
            it = chainhash_set(tmap, tidmap, tid);
            chainhash_value(tmap, it) = arena;
        } else {
            arena = chainhash_value(tmap, it);
        }

        /* let thread's arena handle allocation */
        mem = arena_allocate(arena, n_bytes);
    } else {
        /* round n_bytes to multiple of chunk size */
        n_chunks = (n_bytes + sizeof(nvm_huge_header_t) + CHUNK_SIZE) / CHUNK_SIZE;

        pthread_mutex_lock(&chunk_mtx);
        huge = tree_upper_bound(n_chunks, free_chunks);

        if (huge == NULL) {
            pthread_mutex_unlock(&chunk_mtx);
            nvm_huge = nvm_reserve_huge(n_chunks);
        } else {
            tree_del(&huge->link, &free_chunks);
            pthread_mutex_unlock(&chunk_mtx);

            if (huge->n_chunks > n_chunks) {
                /* got too many chunks, split and insert rest */
                nvm_huge = (nvm_huge_header_t*) ((uintptr_t)huge->nvm_chunk + (huge->n_chunks - n_chunks)*CHUNK_SIZE);
                nvm_huge->state = USAGE_FREE | STATE_INITIALIZED;
                nvm_huge->n_chunks = n_chunks;
                clflush(nvm_huge);
                sfence();

                huge->nvm_chunk->n_chunks -= n_chunks;
                huge->n_chunks -= n_chunks;

                pthread_mutex_lock(&chunk_mtx);
                tree_add(&huge->link, chunk_node_compare, &free_chunks);
                pthread_mutex_unlock(&chunk_mtx);
            } else {
                nvm_huge = huge->nvm_chunk;
                free(huge);
            }
        }
        mem = (void*) (nvm_huge+1);
    }

    return mem;
}

void* nvm_reserve_id(const char *id, uint64_t n_bytes) {
    void *mem = NULL;

    /* check that id is not in use yet */
    if (ot_get(id) != NULL) {
        return NULL;
    }

    if ((mem = nvm_reserve(n_bytes)) == NULL) {
        return NULL;
    }

    ot_insert(id, mem);

    return mem;
}

void nvm_activate(void *ptr, void **link_ptr1, void *target1, void **link_ptr2, void *target2) {
    nvm_huge_header_t *nvm_huge = NULL;
    nvm_block_header_t *nvm_block = NULL;
    nvm_run_header_t *nvm_run = NULL;
    uintptr_t rel_ptr = __NVM_ABS_TO_REL(ptr);
    uint16_t run_idx;

    /* determine whether we are activating a small, large or huge object */
    if (rel_ptr % CHUNK_SIZE == sizeof(nvm_huge_header_t)) {
        /* ptr is 64 bytes into a chunk --> huge block */
        nvm_huge = (nvm_huge_header_t*) ((uintptr_t)ptr - sizeof(nvm_huge_header_t));

        /* store link pointers in header */
        if (link_ptr1) {
            nvm_huge->on[0].ptr = __NVM_ABS_TO_REL(link_ptr1);
            nvm_huge->on[0].value = __NVM_ABS_TO_REL(target1);
            if (link_ptr2) {
                nvm_huge->on[1].ptr = __NVM_ABS_TO_REL(link_ptr2);
                nvm_huge->on[1].value = __NVM_ABS_TO_REL(target2);
            }

            sfence();
            nvm_huge->state = USAGE_HUGE | STATE_ACTIVATING;
            sfence();

            *link_ptr1 = (void*) __NVM_ABS_TO_REL(target1);
            clflush(*link_ptr1);
            if (link_ptr2) {
                *link_ptr2 = (void*) __NVM_ABS_TO_REL(target2);
                clflush(*link_ptr2);
            }

            sfence();
        }

        nvm_huge->state = USAGE_HUGE | STATE_INITIALIZED;
        sfence();
        memset(nvm_huge->on, 0, 2*sizeof(nvm_ptrset_t));
        clflush(nvm_huge);
        sfence();
    } else {
        nvm_block = (nvm_block_header_t*) ((uintptr_t)ptr & ~(BLOCK_SIZE-1));
        if (GET_USAGE(nvm_block->state) == USAGE_FREE) {
            /* large block */

            /* store link pointers in header */
            if (link_ptr1) {
                nvm_block->on[0].ptr = __NVM_ABS_TO_REL(link_ptr1);
                nvm_block->on[0].value = __NVM_ABS_TO_REL(target1);
                if (link_ptr2) {
                    nvm_block->on[1].ptr = __NVM_ABS_TO_REL(link_ptr2);
                    nvm_block->on[1].value = __NVM_ABS_TO_REL(target2);
                }

                sfence();
                nvm_block->state = USAGE_BLOCK | STATE_ACTIVATING;
                sfence();

                *link_ptr1 = (void*) __NVM_ABS_TO_REL(target1);
                clflush(*link_ptr1);
                if (link_ptr2) {
                    *link_ptr2 = (void*) __NVM_ABS_TO_REL(target2);
                    clflush(*link_ptr2);
                }

                sfence();
            }

            nvm_block->state = USAGE_BLOCK | STATE_INITIALIZED;
            sfence();
            memset(nvm_block->on, 0, 2*sizeof(nvm_ptrset_t));
            clflush(nvm_block);
            sfence();
        } else {
            /* small block */
            nvm_run = (nvm_run_header_t*) nvm_block;
            run_idx = ((uintptr_t)ptr - (uintptr_t)(nvm_run+1)) / nvm_run->n_bytes;

            /* save the bit to be changed */
            nvm_run->bit_idx = run_idx;

            /* store link pointers in header */
            if (link_ptr1) {
                nvm_run->on[0].ptr = __NVM_ABS_TO_REL(link_ptr1);
                nvm_run->on[0].value = __NVM_ABS_TO_REL(target1);
                if (link_ptr2) {
                    nvm_run->on[1].ptr = __NVM_ABS_TO_REL(link_ptr2);
                    nvm_run->on[1].value = __NVM_ABS_TO_REL(target2);
                }

                sfence();
                nvm_run->state = USAGE_RUN | STATE_ACTIVATING;
                sfence();

                *link_ptr1 = (void*) __NVM_ABS_TO_REL(target1);
                clflush(*link_ptr1);
                if (link_ptr2) {
                    *link_ptr2 = (void*) __NVM_ABS_TO_REL(target2);
                    clflush(*link_ptr2);
                }
            }

            /* mark slot as used on NVM */
            sfence();
            nvm_run->bitmap[run_idx/8] |= (1<<(run_idx%8));
            sfence();
            nvm_run->state = USAGE_RUN | STATE_INITIALIZED;
            sfence();
            nvm_run->bit_idx = -1;
            memset(nvm_run->on, 0, 2*sizeof(nvm_ptrset_t));
            clflush(nvm_run);
            sfence();
        }
    }
}

void nvm_activate_id(const char *id) {
    object_table_entry_t *ot_entry = NULL;
    nvm_object_table_entry_t *nvm_ot_entry = NULL;

    if ((ot_entry = ot_get(id)) == NULL) {
        return;
    }
    nvm_ot_entry = ot_entry->nvm_entry;

    /* step 1 - persist id in INITIALIZING state */
    nvm_ot_entry->state = STATE_INITIALIZING;
    strncpy(nvm_ot_entry->id, id, MAX_ID_LENGTH);
    nvm_ot_entry->id[54] = '\0';
    nvm_ot_entry->ptr = __NVM_ABS_TO_REL(ot_entry->data_ptr);
    clflush(nvm_ot_entry);
    sfence();

    /* step 2 - activate data normally */
    nvm_activate(ot_entry->data_ptr, NULL, NULL, NULL, NULL);

    /* step 3 - activate NVM object table entry */
    nvm_ot_entry->state = STATE_INITIALIZED;
    clflush(nvm_ot_entry);
    sfence();
}

void* nvm_get_id(const char *id) {
    object_table_entry_t *ot_entry = NULL;

    if ((ot_entry = ot_get(id)) == NULL) {
        return NULL;
    }

    return ot_entry->data_ptr;
}

void nvm_free(void *ptr, void **link_ptr1, void *target1, void **link_ptr2, void *target2) {
    nvm_huge_header_t *nvm_huge = NULL;
    huge_t *huge = NULL;
    uintptr_t rel_ptr = __NVM_ABS_TO_REL(ptr);

    if (rel_ptr % CHUNK_SIZE == sizeof(nvm_huge_header_t)) {
        /* ptr is 64 bytes into a chunk --> huge block */
        nvm_huge = (nvm_huge_header_t*) (ptr - sizeof(nvm_huge_header_t));
        huge = (huge_t*) malloc(sizeof(huge_t));
        huge->nvm_chunk = nvm_huge;
        huge->n_chunks = nvm_huge->n_chunks;

        /* store link pointers in header */
        if (link_ptr1) {
            nvm_huge->on[0].ptr = __NVM_ABS_TO_REL(link_ptr1);
            nvm_huge->on[0].value = __NVM_ABS_TO_REL(target1);
            if (link_ptr2) {
                nvm_huge->on[1].ptr = __NVM_ABS_TO_REL(link_ptr2);
                nvm_huge->on[1].value = __NVM_ABS_TO_REL(target2);
            }

            sfence();
            nvm_huge->state = USAGE_HUGE | STATE_FREEING;
            sfence();

            *link_ptr1 = (void*) __NVM_ABS_TO_REL(target1);
            clflush(*link_ptr1);
            if (link_ptr2) {
                *link_ptr2 = (void*) __NVM_ABS_TO_REL(target2);
                clflush(*link_ptr2);
            }

            sfence();
        }

        nvm_huge->state = USAGE_FREE | STATE_INITIALIZED;
        sfence();
        memset(nvm_huge->on, 0, 2*sizeof(nvm_ptrset_t));
        clflush(nvm_huge);
        sfence();

        pthread_mutex_lock(&chunk_mtx);
        tree_add(&huge->link, chunk_node_compare, &free_chunks);
        pthread_mutex_unlock(&chunk_mtx);
    } else {
        /* otherwise must be a run or block --> let arena handle */
        arena_free(ptr, link_ptr1, target1, link_ptr2, target2);
    }
}

void nvm_free_id(const char *id) {
    object_table_entry_t *ot_entry = NULL;
    nvm_object_table_entry_t *nvm_entry = NULL;

    if ((ot_entry = ot_get(id)) == NULL) {
        return;
    }

    nvm_entry = ot_entry->nvm_entry;
    nvm_entry->state = STATE_FREEING; // TODO: maybe not do this and instead do sanity check on startup?
    clflush(nvm_entry);
    sfence();

    nvm_free(ot_entry->data_ptr, NULL, NULL, NULL, NULL);

    ot_remove(id);
}

extern void nvm_persist(const void *ptr, uint64_t n_bytes) {
    clflush_range(ptr, n_bytes);
    sfence();
}

void* nvm_abs(void *rel_ptr) {
    assert(nvm_start != NULL);
    return (void*) ((uintptr_t)nvm_start + (uintptr_t)rel_ptr);
}

void* nvm_rel(void *abs_ptr) {
    assert(nvm_start != NULL);
    return (void*) ((uintptr_t)abs_ptr - (uintptr_t)nvm_start);
}

/* internal functions */
/* ------------------ */

void nvm_initialize_empty() {
    uint32_t i;
    nvm_chunk_header_t *chunk_hdr=NULL;
    nvm_block_header_t *block_hdr=NULL;
    arena_t *arena=NULL;

    /* perform initialization for chunks when not recovering */
    initialize_chunks();

    /* allocate chunks for the initial arena setup */
    activate_more_chunks(INITIAL_ARENAS);

    /* perform initial chunk setup */
    for (i=0; i<INITIAL_ARENAS; ++i) {
        /* initialize the chunk header */
        chunk_hdr = (nvm_chunk_header_t*)__NVM_REL_TO_ABS(i*CHUNK_SIZE);
        chunk_hdr->state = STATE_INITIALIZING | USAGE_ARENA;
        strncpy(chunk_hdr->signature, NVM_CHUNK_SIGNATURE, 47);
        chunk_hdr->next_ot_chunk = i < INITIAL_ARENAS-1 ? (uintptr_t)((i+1)*CHUNK_SIZE) : (uintptr_t)NULL;
        memset((void*)chunk_hdr->object_table, 0, 4032);
        clflush_range((void*)chunk_hdr, sizeof(nvm_chunk_header_t));
        sfence();

        /* initialize the chunk content */
        block_hdr = (nvm_block_header_t*)(chunk_hdr+1);
        block_hdr->state = STATE_INITIALIZING | USAGE_FREE;
        block_hdr->n_pages = (CHUNK_SIZE - sizeof(nvm_chunk_header_t) - sizeof(nvm_block_header_t)) / BLOCK_SIZE;
        memset((void*)((uintptr_t)block_hdr + 5), 0, 59);
        clflush((void*)block_hdr);
        sfence();
    }
    /* mark the chunks as initialized */
    for (i=0; i<INITIAL_ARENAS; ++i) {
        chunk_hdr = (nvm_chunk_header_t*)__NVM_REL_TO_ABS(i*CHUNK_SIZE);
        chunk_hdr->state = STATE_INITIALIZED | USAGE_ARENA;
        clflush((void*)chunk_hdr);
        sfence();
    }

    /* create arenas on chunks */
    arenas = (arena_t**) malloc(INITIAL_ARENAS * sizeof(arena_t*));
    for (i=0; i<INITIAL_ARENAS; ++i) {
        arena = (arena_t*) malloc(sizeof(arena_t));
        arena_init(arena, i, __NVM_REL_TO_ABS(i*CHUNK_SIZE), 1);
        arenas[i] = arena;
    }
}

void nvm_initialize_recovered(uint64_t n_chunks_recovered) {
    uint64_t i;
    arena_t *arena;
    nvm_chunk_header_t *nvm_chunk = (nvm_chunk_header_t*) nvm_start;
    nvm_huge_header_t *nvm_huge = NULL;
    huge_t *huge = NULL;

    /* process the initial arenas chunks */
    arenas = (arena_t**) malloc(INITIAL_ARENAS * sizeof(arena_t*));
    for (i=0; i<INITIAL_ARENAS; ++i) {
        arena = (arena_t*) malloc(sizeof(arena_t));
        arena->id = i;
        arena_recover(arena, i, nvm_chunk); // TODO: do this in separate threads
        arenas[i] = arena;

        nvm_chunk = (nvm_chunk_header_t*) ((uintptr_t)nvm_chunk + CHUNK_SIZE);
    }

    /* process remaining chunks, looking for huge objects */
    for (; i<n_chunks_recovered; ++i) {
        nvm_chunk = (nvm_chunk_header_t*) ((uintptr_t)nvm_start + i*CHUNK_SIZE);
        /* we are not interested in arena chunks */
        if (GET_USAGE(nvm_chunk->state) == USAGE_HUGE) {
            nvm_huge = (nvm_huge_header_t*) nvm_chunk;

            huge = (huge_t*) malloc(sizeof(huge_t));
            huge->nvm_chunk = nvm_huge;
            huge->n_chunks = nvm_huge->n_chunks;

            if (GET_STATE(nvm_chunk->state) != STATE_INITIALIZED) {
                // TODO: check if we can replay the allocation here?
                tree_add(&huge->link, chunk_node_compare, &free_chunks);
            }

            //nvm_chunk = (nvm_chunk_header_t*) ((uintptr_t)nvm_chunk + huge->n_chunks * CHUNK_SIZE);
            i += huge->n_chunks - 1;
            huge = NULL;
            nvm_huge = NULL;
        }
    }
}

nvm_huge_header_t* nvm_reserve_huge(uint64_t n_chunks) {
    nvm_huge_header_t *nvm_huge = NULL;

    /* create new chunks for the request */
    nvm_huge = activate_more_chunks(n_chunks);
    nvm_huge->state = USAGE_HUGE | STATE_INITIALIZING;
    nvm_huge->n_chunks = n_chunks;
    memset(nvm_huge->on, 0, sizeof(nvm_huge->on));
    clflush(nvm_huge);
    sfence();

    return nvm_huge;
}
