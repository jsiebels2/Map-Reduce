#include "mr.h"

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "hash.h"
#include "kvlist.h"

typedef struct {
  mapper_t mapper;
  kvlist_t* in;
  kvlist_t* out;
} map_arg;

typedef struct {
  reducer_t reducer;
  kvlist_t* in;
  kvlist_t* out;
} reduce_arg;

void* map_worker(void* arg) {
  map_arg* a = (map_arg*)arg;
  kvlist_iterator_t* it = kvlist_iterator_new(a->in);
  kvpair_t* pair;

  while ((pair = kvlist_iterator_next(it)) != NULL) {
    a->mapper(pair, a->out);
  }

  kvlist_iterator_free(&it);
  return NULL;
}

void* reduce_worker(void* arg) {
  reduce_arg* a = (reduce_arg*)arg;
  kvlist_sort(a->in);

  kvlist_iterator_t* it = kvlist_iterator_new(a->in);
  kvlist_t* run = kvlist_new();
  char* cur_key = NULL;
  kvpair_t* pair;

  while ((pair = kvlist_iterator_next(it)) != NULL) {
    if (cur_key != NULL && strcmp(cur_key, pair->key) != 0) {
      a->reducer(cur_key, run, a->out);
      kvlist_free(&run);
      run = kvlist_new();
    }
    cur_key = pair->key;
    kvlist_append(run, kvpair_new(pair->key, pair->value));
  }

  if (cur_key != NULL) {
    a->reducer(cur_key, run, a->out);
  }

  kvlist_free(&run);
  kvlist_iterator_free(&it);
  return NULL;
}

void map_reduce(mapper_t mapper, size_t num_mapper, reducer_t reducer,
                size_t num_reducer, kvlist_t* input, kvlist_t* output) {
  kvlist_t** lists = malloc(num_mapper * sizeof(kvlist_t*));
  for (size_t i = 0; i < num_mapper; i++) {
    lists[i] = kvlist_new();
  }

  kvlist_iterator_t* it = kvlist_iterator_new(input);
  kvpair_t* pair;
  size_t i = 0;
  while ((pair = kvlist_iterator_next(it)) != NULL) {
    kvlist_append(lists[i % num_mapper], kvpair_new(pair->key, pair->value));
    i++;
  }
  kvlist_iterator_free(&it);

  kvlist_t** map_out = malloc(num_mapper * sizeof(kvlist_t*));
  pthread_t* map_threads = malloc(num_mapper * sizeof(pthread_t));
  map_arg* margs = malloc(num_mapper * sizeof(map_arg));

  for (size_t i = 0; i < num_mapper; i++) {
    map_out[i] = kvlist_new();
    margs[i].mapper = mapper;
    margs[i].in = lists[i];
    margs[i].out = map_out[i];
    pthread_create(&map_threads[i], NULL, map_worker, &margs[i]);
  }

  for (size_t i = 0; i < num_mapper; i++) {
    pthread_join(map_threads[i], NULL);
  }

  for (size_t i = 0; i < num_mapper; i++) kvlist_free(&lists[i]);
  free(lists);
  free(margs);
  free(map_threads);

  kvlist_t** buckets = malloc(num_reducer * sizeof(kvlist_t*));
  for (size_t i = 0; i < num_reducer; i++) {
    buckets[i] = kvlist_new();
  }

  for (size_t i = 0; i < num_mapper; i++) {
    kvlist_iterator_t* it = kvlist_iterator_new(map_out[i]);
    kvpair_t* pair;
    while ((pair = kvlist_iterator_next(it)) != NULL) {
      size_t b = hash(pair->key) % num_reducer;
      kvlist_append(buckets[b], kvpair_new(pair->key, pair->value));
    }
    kvlist_iterator_free(&it);
  }

  for (size_t i = 0; i < num_mapper; i++) kvlist_free(&map_out[i]);
  free(map_out);

  kvlist_t** red_out = malloc(num_reducer * sizeof(kvlist_t*));
  pthread_t* red_threads = malloc(num_reducer * sizeof(pthread_t));
  reduce_arg* rargs = malloc(num_reducer * sizeof(reduce_arg));

  for (size_t i = 0; i < num_reducer; i++) {
    red_out[i] = kvlist_new();
    rargs[i].reducer = reducer;
    rargs[i].in = buckets[i];
    rargs[i].out = red_out[i];
    pthread_create(&red_threads[i], NULL, reduce_worker, &rargs[i]);
  }
  for (size_t i = 0; i < num_reducer; i++) {
    pthread_join(red_threads[i], NULL);
  }

  for (size_t i = 0; i < num_reducer; i++) {
    kvlist_extend(output, red_out[i]);
    kvlist_free(&red_out[i]);
    kvlist_free(&buckets[i]);
  }
  free(red_out);
  free(buckets);
  free(rargs);
  free(red_threads);
}
