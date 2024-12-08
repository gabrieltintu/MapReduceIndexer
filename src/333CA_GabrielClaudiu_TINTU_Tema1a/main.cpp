#include "functions.hpp"

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
			cerr << "Error creating thread " << i << endl;
			exit(-1);
		}
	}

	// wait for all threads to finish
	for (int i = 0; i < mappers + reducers; i++) {
		int r = pthread_join(threads[i], NULL);

		if (r) {
			cerr << "Error while waiting for the thread " << i << endl;
			exit(-1);
		}
	}

	pthread_mutex_destroy(&mutex);
	pthread_barrier_destroy(&barrier);
	pthread_barrier_destroy(&reducer_barrier);

	return 0;
}