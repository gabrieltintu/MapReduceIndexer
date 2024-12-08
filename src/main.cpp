#include <iostream>
#include <fstream>
#include <cstring>
#include <vector>
#include <set>
#include "pthread_barrier_mac.h"
#include <pthread.h>
#include <unordered_map>
#include <map>
#include <algorithm>
#include <cmath>

using namespace std;

struct thread_args_t {
	vector<string> files; // files to be processed
	int mappers;		  // number of mapper threads
	int reducers;		  // number of reducer thread
	int *file_index;	  // shared index tracking the current file being processed

	vector<unordered_map<string, int>>* partial_maps;    // vector of intermediate maps for each file
	vector<unordered_map<string, set<int>>>* final_maps; // final structure with words grouped by their first letter

	int thread_id; // ID of the current thread
	pthread_mutex_t *mutex;
	pthread_barrier_t *barrier;
	pthread_barrier_t *reducer_barrier;
};

int min(int a, int b) {
	return a <= b ? a : b;
}

bool compare_func(pair<string, set<int>> a, pair<string, set<int>> b) {
	if (a.second.size() != b.second.size())
		return a.second.size() > b.second.size();

	return a.first < b.first;
}

void parse_input(char **argv, int &mappers, int &reducers, vector<string>& files, int& num_files)
{
	mappers = stoi(argv[1]);
	reducers = stoi(argv[2]);
	string in_file = argv[3];

	ifstream fin(in_file);

	if (!fin) {
		cerr << "Error: Could not open input file " << in_file << endl;
		exit(-1);
	}

	fin >> num_files;

	for (int i = 0; i < num_files; i++) {
		string file;
		fin >> file;

		files.push_back(file); 
	}

	fin.close();
}

bool filter_and_convert(char& c) {
	if (isalpha(c)) {
		c = tolower(c);
		return false;
	}

	return true;
}

void *map_function(void *args) {
	thread_args_t* thread_args = (thread_args_t *)(args);

	while(true) {
		pthread_mutex_lock(thread_args->mutex);

		// check if there are still files left to process
		if (*thread_args->file_index >= thread_args->files.size()) {
			pthread_mutex_unlock(thread_args->mutex);
			break;
		}

		// get the current file index and increment it for the next file
		int curr_file = *thread_args->file_index + 1;
		string file = thread_args->files[*thread_args->file_index];
		(*thread_args->file_index)++;

		pthread_mutex_unlock(thread_args->mutex);

		string word;
		string filename = "../checker/" + file;
		ifstream fin(filename);    

		// map to store word occurrences for the current file
		unordered_map<string, int> partial_map;

		while (fin >> word)
		{
			// remove non-alpha charss from the word
			// convert remaining characters to lowercase.
			word.erase(remove_if(word.begin(), word.end(), filter_and_convert), word.end());

			if (word.empty())
				continue;

			partial_map[word] = curr_file;
		}

		// add the partial map to the array 
		pthread_mutex_lock(thread_args->mutex);
		thread_args->partial_maps->push_back(partial_map);
		pthread_mutex_unlock(thread_args->mutex);
		
		fin.close();
	}

	return 0;
}

void *reduce_function(void *args) {
	thread_args_t* thread_args = (thread_args_t *)(args);

	// range of partial maps this thread is responsible for
	int n = (*thread_args->partial_maps).size();
	int p = thread_args->reducers;
	int start = (thread_args->thread_id - thread_args->mappers) * (double)n / p;
	int end = min((thread_args->thread_id - thread_args->mappers + 1) * (double)n / p, n);

	// iterate through the assigned range of partial maps
	for (int i = start; i < end; i++) {
		unordered_map<int, unordered_map<string, set<int>>> local_maps;

		// group words from each partial map by their first letter and store them in local_maps
		for (auto& el : (*thread_args->partial_maps)[i]) {
			char first_letter = el.first[0];
			int index = first_letter - 'a';

			local_maps[index][el.first].insert(el.second);
		}

		for (auto& map : local_maps) {
			pthread_mutex_lock(thread_args->mutex);
			for (auto& el : map.second) {
				(*thread_args->final_maps)[map.first][el.first].insert(el.second.begin(), el.second.end());
			}
			pthread_mutex_unlock(thread_args->mutex);
		}
	}

	// wait for all reducers to complete processing the partial maps
	pthread_barrier_wait(thread_args->reducer_barrier);

	// range of letter indices this thread is responsible for
	int num_maps = 26;
	int new_start = (thread_args->thread_id - thread_args->mappers) * (double)num_maps / p;
	int new_end = min((thread_args->thread_id - thread_args->mappers + 1) * (double)num_maps / p, num_maps);

	for (int i = new_start; i < new_end; i ++) {
		auto& curr_map = (*thread_args->final_maps)[i];
		
		// convert the map to a vector to allow sorting 
		vector<pair<string, set<int>>> sorted_entries(curr_map.begin(), curr_map.end());
		sort(sorted_entries.begin(), sorted_entries.end(), compare_func);

		// open a file with the corresponding letter and write the words and the list of files
		char letter = 'a' + i;
		string filename = string(1, letter) + ".txt";
		ofstream fout(filename); 

		for (auto& el : sorted_entries) {
			fout << el.first << ":[";
			int count = 0;

			for (auto& num : el.second) {
				fout << num;
				if (count != el.second.size() - 1)
					fout << " ";
				count++;
			}

			fout << "]" << endl;
		}

		fout.close();
	}

	return 0;
}

void *func(void *args) {
	thread_args_t* thread_args = (thread_args_t *)(args);

	if (thread_args->thread_id < thread_args->mappers)
		map_function(args);

	// wait for all mappers to finish before starting reducers
	pthread_barrier_wait(thread_args->barrier);

	if (thread_args->thread_id >= thread_args->mappers)
		reduce_function(args);

	pthread_exit(NULL);
}

int main(int argc, char **argv)
{   
	if (argc < 4) {
		cerr << "Usage: " << argv[0] << " <num_mappers> <num_reducers> <input_file>" << endl;
		return 1;
	}

	vector<string> files;
	int mappers, reducers, num_files;
	parse_input(argv, mappers, reducers, files, num_files);
	
	pthread_t threads[mappers + reducers];
	thread_args_t thread_args[mappers + reducers];

	pthread_mutex_t mutex;
	pthread_barrier_t barrier, reducer_barrier;

	pthread_mutex_init(&mutex, NULL);
	pthread_barrier_init(&barrier, NULL, mappers + reducers);
	pthread_barrier_init(&reducer_barrier, NULL, reducers);

	int file_index = 0;

	vector<unordered_map<string, int>> partial_maps;
	vector<unordered_map<string, set<int>>> final_maps(26);

	// create and initialize mappers and reducers
	for (int i = 0; i < mappers + reducers; i++) {
		thread_args[i].mappers = mappers;
		thread_args[i].reducers = reducers;
		thread_args[i].thread_id = i;
		thread_args[i].files = files;
	
		thread_args[i].mutex = &mutex;
		thread_args[i].barrier = &barrier;
		thread_args[i].reducer_barrier = &reducer_barrier;
		thread_args[i].file_index = &file_index;
		thread_args[i].partial_maps = &partial_maps;
		thread_args[i].final_maps = &final_maps;

		// create thread and pass in the corresponding thread_args
		int r = pthread_create(&threads[i], NULL, func, &thread_args[i]);
		if (r) {
			printf("Error creating thread %d\n", i);
			exit(-1);
		}
	}

	// wait for all threads to finish
	for (int i = 0; i < mappers + reducers; i++) {
		int r = pthread_join(threads[i], NULL);

		if (r) {
			printf("Error while waiting for the thread %d\n", i);
			exit(-1);
		}
	}

	int i = 0;

	pthread_mutex_destroy(&mutex);
	pthread_barrier_destroy(&barrier);

	return 0;
}