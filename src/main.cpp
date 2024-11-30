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
	pthread_barrier_t *reducer_barrier;
	vector<map<string, set<int>>>* final_map;
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

void trim(string& s) {
    // Remove leading spaces
    s.erase(s.begin(), find_if(s.begin(), s.end(), [](unsigned char ch) {
        return !isspace(ch);
    }));
    // Remove trailing spaces
    s.erase(find_if(s.rbegin(), s.rend(), [](unsigned char ch) {
        return !isspace(ch);
    }).base(), s.end());
}

void *map_function(void *args) {
	thread_args_t* thread_args = (thread_args_t *)(args);

	while(true) {
		pthread_mutex_lock(thread_args->mutex);

		if (*thread_args->file_index >= thread_args->files.size()) {
			pthread_mutex_unlock(thread_args->mutex);
			break;
		}

		int curr_file = *thread_args->file_index + 1;
		string file = thread_args->files[*thread_args->file_index];
		(*thread_args->file_index)++;

		pthread_mutex_unlock(thread_args->mutex);

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
			trim(word);

			if (word.empty())
				continue;

			for (auto& c : word)
				c = tolower(c);

			word.erase(remove_if(word.begin(), word.end(),
                             [](char c) {
                                 return !isalpha(c);
                             }),
                   word.end());
			// cout << word << endl;
			if (word.empty())
				continue;
			partial_map[word] = curr_file;
		}

		// for (auto el : partial_map) {
		// 	cout << el.first << " " << el.second << endl;
		// }
		pthread_mutex_lock(thread_args->mutex);
		thread_args->partial_maps->push_back(partial_map);
		pthread_mutex_unlock(thread_args->mutex);
		
		fin.close();
	}
	// cout << "thread: " << thread_args->thread_id << endl;
	// pthread_exit(NULL);
	return 0;
}

int min(int a, int b) {
	return a <= b ? a : b;
}

bool compare_by_size(pair<string, set<int>> a, pair<string, set<int>> b) {
    if (a.second.size() != b.second.size()) {
        return a.second.size() > b.second.size();
    }

    return a.first < b.first;
}

void *reduce_function(void *args) {
    thread_args_t* thread_args = (thread_args_t *)(args);
    cout << "aici\n";
    // Calculul intervalului pentru acest thread
    int n = (*thread_args->partial_maps).size();
	int p = thread_args->reducers;
	int start = (thread_args->thread_id - thread_args->mappers) * (double)n / p;
	int end = min((thread_args->thread_id - thread_args->mappers + 1) * (double)n / p, n);

    // Populare final_map
    for (int i = start; i < end; i++) {
		unordered_map<int, unordered_map<string, set<int>>> local_map;

		for (auto& el : (*thread_args->partial_maps)[i]) {
			char first_letter = el.first[0];
			int index = first_letter - 'a';

			if (local_map.find(index) == local_map.end()) {
				local_map[index] = unordered_map<string, set<int>>();
			}

			local_map[index][el.first].insert(el.second);
		}

		pthread_mutex_lock(thread_args->mutex);

		for (auto& kv : local_map) {
			if ((*thread_args->final_map).size() <= kv.first) {
				(*thread_args->final_map).resize(kv.first + 1);
			}
			
			for (auto& sub_kv : kv.second) {
				(*thread_args->final_map)[kv.first][sub_kv.first].insert(sub_kv.second.begin(), sub_kv.second.end());
			}
		}

		pthread_mutex_unlock(thread_args->mutex);
	}


	cout << "mai jos????\n";
    pthread_barrier_wait(thread_args->reducer_barrier);

	
    int num_maps = 26;
	int new_start = (thread_args->thread_id - thread_args->mappers) * (double)num_maps / p;
	
	int new_end = min((thread_args->thread_id - thread_args->mappers + 1) * (double)num_maps / p, num_maps);
	cout << new_start << " " << new_end << endl;
    for (int i = new_start; i < new_end; i ++) {
        auto& curr_map = (*thread_args->final_map)[i];

        vector<pair<string, set<int>>> sorted_entries(curr_map.begin(), curr_map.end());

		sort(sorted_entries.begin(), sorted_entries.end(), compare_by_size);

		char letter = 'a' + i;
        string filename = string(1, letter) + ".txt";

        ofstream fout(filename, ios::app); 

        for (const auto& el : sorted_entries) {
            fout << el.first << ":[";
            int count = 0;
            for (const auto& num : el.second) {
                fout << num;
                if (count != el.second.size() - 1)
                    fout << " ";
                count++;
            }
            fout << "]" << endl;
        }

        fout.close();

        // curr_map.clear();
        // for (const auto& el : sorted_entries) {
		// // for (int j = sorted_entries.size() - 1; j >= 0; j--) {
        //     // curr_map[sorted_entries[j].first] = sorted_entries[j].second;
        //     curr_map[el.first] = el.second;
        // }
    }

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
	pthread_barrier_t reducer_barrier;


	pthread_mutex_init(&mutex, NULL);
	pthread_barrier_init(&barrier, NULL, mappers + reducers);
	pthread_barrier_init(&reducer_barrier, NULL, reducers);



	int file_index = 0;
	vector<unordered_map<string, int>> maps;

	vector<map<string, set<int>>> final_map(26);

	for (int i = 0; i < mappers + reducers; i++) {
		thread_args[i].mappers = mappers;
		thread_args[i].reducers = reducers;
		thread_args[i].files = files;
		thread_args[i].file_index = &file_index;
		thread_args[i].mutex = &mutex;
		thread_args[i].barrier = &barrier;
		thread_args[i].reducer_barrier = &reducer_barrier;

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

	// vector<pair<string, set<int>>> sorted_map(final_map.begin(), final_map.end());


    // sort(sorted_map.begin(), sorted_map.end(), compare_by_size);

	// for (int i = 0; i < 26; i++) {
	// 	char letter = 'a' + i;  // Determine the corresponding letter
	// 	string filename = string(1, letter) + ".txt";

	// 	ofstream fout(filename, ios::app);  // Open file for appending

	// 	for (const auto& el : final_map[i]) {
	// 		fout << el.first << ":[";
	// 		int count = 0;
	// 		for (const auto& num : el.second) {
	// 			fout << num;
	// 			if (count != el.second.size() - 1)
	// 				fout << " ";
	// 			count++;
	// 		}
	// 		fout << "]" << endl;
    // 	}

    // 	fout.close();
	// }

	pthread_mutex_destroy(&mutex);
	pthread_barrier_destroy(&barrier);

	return 0;
}