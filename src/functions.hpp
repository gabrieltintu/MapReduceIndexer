#pragma once

#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <set>
#include "pthread_barrier_mac.h"
#include <pthread.h>
#include <unordered_map>
#include <algorithm>

using namespace std;

struct thread_args_t {
	vector<string> files; // files to be processed
	int mappers;		  // number of mapper threads
	int reducers;		  // number of reducer thread
	int *file_index;	  // shared index tracking the current file being processed

	vector<unordered_map<string, int>>* partial_maps;    // vector of intermediate maps for each file
	vector<unordered_map<string, set<int>>>* final_maps; // final structure with words grouped by their first letter

	int thread_id;                      // ID of the current thread
	pthread_mutex_t *mutex;             // mutex for thread sync
	pthread_barrier_t *barrier;         // barrier for mappers + reducers
	pthread_barrier_t *reducer_barrier; // barrier for reducers
};

int min(int a, int b);
bool compare_func(pair<string, set<int>> a, pair<string, set<int>> b);
void parse_input(char **argv, int &mappers, int &reducers, vector<string>& files, int& num_files);
void *map_function(void *args);
void *reduce_function(void *args);
void *func(void *args);