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
	vector<string> files;
	int mappers;
	int reducers;
	vector<unordered_map<string, int>>* partial_maps;
	pthread_mutex_t *mutex;
	pthread_barrier_t *barrier;
	map<string, set<int>>* final_map;
	int *file_index;
	int thread_id;
};

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

void *map_function(void *args) {
	thread_args_t* thread_args = (thread_args_t *)(args);

	while(true) {
		int ret = pthread_mutex_lock(thread_args->mutex);
		if (ret) {
			printf("error\n");
			pthread_exit(NULL);
		}

		if (*thread_args->file_index >= thread_args->files.size()) {
			pthread_mutex_unlock(thread_args->mutex);
			break;
		}

		int curr_file = *thread_args->file_index + 1;
		string file = thread_args->files[*thread_args->file_index];
		(*thread_args->file_index)++;

		// pthread_mutex_unlock(thread_args->mutex);

		string word;
		string filename = "../checker/" + file;
		ifstream fin(filename);    
		// f.open(filename.c_str());

	
		cout << "Thread " << thread_args->thread_id << " processing file: " << file 
			 << " (file index: " << *thread_args->file_index << ")" << endl;

		// pthread_mutex_unlock(thread_args->mutex);
		
		string str = "',.";

		unordered_map<string, int> partial_map;
		while (fin >> word)
		{
			for (auto& c : word)
				c = tolower(c);

			word.erase(remove_if(word.begin(), word.end(),
                             [](char c) {
                                 return !isalpha(c);
                             }),
                   word.end());
			// cout << word << endl;
			partial_map[word] = curr_file;
		}

		// for (auto el : partial_map) {
		// 	cout << el.first << " " << el.second << endl;
		// }

		thread_args->partial_maps->push_back(partial_map);
		fin.close();

		pthread_mutex_unlock(thread_args->mutex);

	}
	// cout << "thread: " << thread_args->thread_id << endl;
	// pthread_exit(NULL);
	return 0;
}

int min(int a, int b) {
	return a <= b ? a : b;
}

void *reduce_function(void *args) {
	thread_args_t* thread_args = (thread_args_t *)(args);
	int i = 0;
	// cout << "threadid : "<< thread_args->thread_id<<endl;
	// for (auto map : *thread_args->partial_maps) {
	// 	cout << i << endl;
	// 	// cout << "Reducer " << thread_args->thread_id << " processing map " << i << endl;

	// 	for (auto el : map) {
	// 		cout << el.first << " " << el.second << endl;
	// 	}
	// 	i++;
	// }
	int n = (*thread_args->partial_maps).size();
	int p = thread_args->reducers;
	int start = (thread_args->thread_id - thread_args->mappers) * (double)n / p;
	int end = min((thread_args->thread_id - thread_args->mappers + 1) * (double)n / p, n);


	for (int i = start; i < end; i++) {
		// pthread_mutex_lock(thread_args->mutex);
       	// 	cout << i << endl;
		// pthread_mutex_unlock(thread_args->mutex);
		// for (auto map : *thread_args->partial_maps) {
		for (auto el : (*thread_args->partial_maps)[i]) {
			 
			pthread_mutex_lock(thread_args->mutex);
       		(*thread_args->final_map)[el.first].insert(el.second);
			pthread_mutex_unlock(thread_args->mutex);
			// cout << el.first << endl;
		}
	}

	// return 0;
	pthread_exit(NULL);
}

void *func(void *args) {
	thread_args_t* thread_args = (thread_args_t *)(args);

	if (thread_args->thread_id < thread_args->mappers) {
		map_function(args);
		// cout << "Mapper " << thread_args->thread_id << " waiting at barrier.\n";
	}
	
	// cout << "thread: " << thread_args->thread_id << endl;

	pthread_barrier_wait(thread_args->barrier);

	if (thread_args->thread_id >= thread_args->mappers) {
		// cout << "Reducer " << thread_args->thread_id << " starting reduce operation.\n";

		reduce_function(args);
	}
	

	pthread_exit(NULL);
}

bool compare_by_size(pair<string, set<int>> a, pair<string, set<int>> b) {
    if (a.second.size() != b.second.size())
        return a.second.size() > b.second.size();
    
    // If the sizes are equal, sort alphabetically
    return a.first < b.first;
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
	
	
	pthread_t *threads = new pthread_t[mappers + reducers];
	thread_args_t *thread_args = new thread_args_t[mappers + reducers];

	pthread_mutex_t mutex;
	pthread_barrier_t barrier;

	pthread_mutex_init(&mutex, NULL);
	pthread_barrier_init(&barrier, NULL, mappers + reducers);


	int file_index = 0;
	vector<unordered_map<string, int>> maps;

	map<string, set<int>> final_map;

	for (int i = 0; i < mappers + reducers; i++) {
		thread_args[i].mappers = mappers;
		thread_args[i].reducers = reducers;
		thread_args[i].files = files;
		thread_args[i].file_index = &file_index;
		thread_args[i].mutex = &mutex;
		thread_args[i].barrier = &barrier;
		thread_args[i].partial_maps = &maps;
		thread_args[i].thread_id = i;
		thread_args[i].final_map = &final_map;

		int r = pthread_create(&threads[i], NULL, func, &thread_args[i]);
		if (r) {
			printf("Error creating thread %d\n", i);
			exit(-1);
		}
	}


	for (int i = 0; i < mappers + reducers; i++) {
		int r = pthread_join(threads[i], NULL);

		if (r) {
			printf("Eroare la asteptarea thread-ului %d\n", i);
			exit(-1);
		}
	}

	int i = 0;
	
	// for (auto el : sorted_map) {
	// 	cout << el.first;
	// 	for (auto it : el.second) {
	// 		cout << " " << it;
	// 	}
	// 	cout << endl;
	// }
	vector<pair<string, set<int>>> sorted_map(final_map.begin(), final_map.end());


    sort(sorted_map.begin(), sorted_map.end(), compare_by_size);

	for (auto el : sorted_map) {
		// cout << el.first;
		string filename = string(1, el.first[0]) + ".txt";
		// cout << filename << endl;
		ofstream fout(filename, ios::app);
		fout << el.first << ":[";
		int i = 0;
		for (auto it : el.second) {
			// cout << " "<<it;
			fout << it;
			if (i != el.second.size() - 1)
				fout << " ";
			i++;
		}
		fout << "]" << endl;
		// cout << endl;
		fout.close();
	}
	// cout << *thread_args[0].file_index << endl;

	pthread_mutex_destroy(&mutex);
	pthread_barrier_destroy(&barrier);


	// cout << mappers << " " << reducers << endl;
	// for (auto file : files)
	//     cout << file << endl;
   

	return 0;
}