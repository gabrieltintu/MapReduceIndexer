#include "functions.hpp"

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

		int file_index = *thread_args->file_index;
		int num_files = thread_args->files.size();

		// check if there are still files left to process
		if (file_index >= num_files) {
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
			// remove non-alpha chars from the word
			// convert alpha chars to lowercase
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
				if (count != (int)el.second.size() - 1)
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