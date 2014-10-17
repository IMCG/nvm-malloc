/* Copyright (c) 2014 Tim Berning */

#include "object_table.h"

#include <assert.h>
#include <string.h>
#include <stdio.h>

#include <ulib/hash_chain_prot.h>

#define NVM_ABS_TO_REL(base, ptr) ((uintptr_t)ptr - (uintptr_t)base)
#define NVM_REL_TO_ABS(base, ptr) (void*)((uintptr_t)base + (uintptr_t)ptr)

/* free slot fifo queue to store freed positions */
#define SLOT_BUFFER_SIZE 200
static uint64_t slot_buffer[SLOT_BUFFER_SIZE];
static int64_t slot_buffer_next_idx = 0;
static int64_t slot_buffer_head_idx = 0;
static int64_t slot_buffer_tail_idx = -1;
static int16_t slot_buffer_n_free = 0;

/* object table hash map components */

inline uint64_t hash_fn(const char *str) {
    uint64_t hash = 5381;
    int c;
    while ((c = *str++)) {
        hash = ((hash << 5) + hash) + c; /* hash * 33 + c */
    }
    return hash;
}

inline int equal_fn(const char *a, const char *b) {
    return strncmp(a, b, MAX_ID_LENGTH) == 0;
}

inline int compare_fn(const char *a, const char *b) {
    return strncmp(a, b, MAX_ID_LENGTH);
}

DEFINE_CHAINHASH(ot,                    /*name*/
                 const char*,           /*keytype*/
                 object_table_entry_t*, /*valtype*/
                 1,                     /*ismap*/
                 hash_fn,               /*hashfn*/
                 equal_fn,              /*equalfn*/
                 compare_fn);           /*cmpfn*/

static chainhash_t(ot) *ot_hashmap = NULL;
static uint64_t total_slots_available = 0;
static uint64_t next_nvm_slot = 0;
static void* first_chunk = NULL;

#define HASHMAP_INSERT(key, value) \
  { chainhash_itr_t(ot) it = chainhash_set(ot, ot_hashmap, key); \
    assert(!chainhash_end(it)); \
    chainhash_value(ot, it) = value; }

void ot_init(void *nvm_start) {
    ot_hashmap = chainhash_init(ot, 100);
    first_chunk = nvm_start;
    nvm_chunk_header_t *chunk_hdr = (nvm_chunk_header_t*) nvm_start;
    while (1) {
        total_slots_available += 63;
        if (chunk_hdr->next_ot_chunk == (uintptr_t)NULL) {
            break;
        } else{
            chunk_hdr = (nvm_chunk_header_t*) NVM_REL_TO_ABS(nvm_start, chunk_hdr->next_ot_chunk);
        }
    }
}

void ot_recover() {
    int i=0;
    uint16_t n;
    uint64_t current_slot = 0;
    uint64_t last_used_slot = 0;
    nvm_chunk_header_t *chunk_hdr = (nvm_chunk_header_t*) first_chunk;
    nvm_object_table_entry_t *nvm_entry = NULL;
    object_table_entry_t *entry = NULL;

    while (1) {
        //nvm_entry = chunk_hdr->object_table;
        for (i=0; i<63; ++i) {
            nvm_entry = &chunk_hdr->object_table[i];
            if (nvm_entry->state == STATE_INITIALIZED) {
                entry = malloc(sizeof(object_table_entry_t));
                memcpy(entry->id, nvm_entry->id, MAX_ID_LENGTH);
                entry->slot = current_slot;
                entry->data_ptr = NVM_REL_TO_ABS(first_chunk, nvm_entry->ptr);
                entry->nvm_entry = nvm_entry;
                for (n=last_used_slot+1; n<current_slot; ++n) {
                    slot_buffer[slot_buffer_next_idx++] = n;
                    ++slot_buffer_tail_idx;
                    ++slot_buffer_n_free;
                }
                last_used_slot = current_slot;
                HASHMAP_INSERT(entry->id, entry);
            }
            ++current_slot;
        }
        if (chunk_hdr->next_ot_chunk) {
            chunk_hdr = (nvm_chunk_header_t*) NVM_REL_TO_ABS(first_chunk, chunk_hdr->next_ot_chunk);
        } else {
            break;
        }
    }

    next_nvm_slot = last_used_slot+1;
}

int ot_insert(const char *id, void *data_ptr) {
    uint64_t slot, chunk_for_slot, slot_in_chunk;
    object_table_entry_t *entry = NULL;
    nvm_chunk_header_t *chunk_hdr = NULL;
    nvm_object_table_entry_t *nvm_entry = NULL;

    if (ot_get(id) != NULL) {
        return OT_DUPLICATE;
    }

    /* find and reserve a slot on NVM, check freed slots first */
    if (__sync_fetch_and_sub(&slot_buffer_n_free, 1) <= 0) {
        slot_buffer_n_free += 1;
        if ((slot = __sync_fetch_and_add(&next_nvm_slot, 1)) >= total_slots_available) {
            /* oops, we ran out of slots... */
            __sync_fetch_and_sub(&next_nvm_slot, 1);
            return OT_FAIL;
        }
    } else {
        slot = slot_buffer[__sync_fetch_and_add(&slot_buffer_head_idx, 1)];
    }

    /* determine NVM location for OT entry */
    chunk_for_slot = slot / 63;
    slot_in_chunk = slot % 63;
    chunk_hdr = (nvm_chunk_header_t*)((uintptr_t)first_chunk + chunk_for_slot*CHUNK_SIZE);
    nvm_entry = &chunk_hdr->object_table[slot_in_chunk];

    /* create the volatile OT entry */
    entry = (object_table_entry_t*) malloc(sizeof(object_table_entry_t));
    strncpy(entry->id, id, MAX_ID_LENGTH);
    entry->id[MAX_ID_LENGTH] = '\0';
    entry->slot = slot;
    entry->data_ptr = data_ptr;
    entry->nvm_entry = nvm_entry;

    /* insert into the hashmap */
    HASHMAP_INSERT(entry->id, entry);
    return OT_OK;
}

object_table_entry_t* ot_get(const char *id) {
    chainhash_itr_t(ot) it = chainhash_get(ot, ot_hashmap, id);
    if (!chainhash_end(it)) {
        return chainhash_value(ot, it);
    } else {
        return (object_table_entry_t*)NULL;
    }
}

int ot_remove(const char *id) {
    object_table_entry_t *entry;
    uint64_t free_slot_idx;
    chainhash_itr_t(ot) it = chainhash_get(ot, ot_hashmap, id);
    if (!chainhash_end(it)) {
        entry = chainhash_value(ot, it);
        chainhash_del(ot, it);
        free_slot_idx = __sync_fetch_and_add(&slot_buffer_next_idx, 1) % SLOT_BUFFER_SIZE;
        slot_buffer[free_slot_idx] = entry->slot;
        while (!__sync_bool_compare_and_swap(&slot_buffer_tail_idx, free_slot_idx-1, free_slot_idx)) {}
        slot_buffer_n_free += 1;
        free((void*)entry);
        return OT_OK;
    } else {
        return OT_FAIL;
    }
}
