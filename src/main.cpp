#include <iostream>
#include <fstream>
#include <cstring>
#include <vector>
#include <set>
#include "pthread_barrier_mac.h"
#include <pthread.h>
#include <unordered_map>

using namespace std;

struct thread_args_t {
    vector<string> files;
    unordered_map<string, set<int>> partial_map;
    pthread_mutex_t *mutex;
    pthread_barrier_t *barrier;
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
}

void *map_function(void *args) {
    thread_args_t* thread_args = (thread_args_t *)(args);

    while(true) {
        // Lock pentru a extrage următorul fișier
        int ret = pthread_mutex_lock(thread_args->mutex);
        if (ret) {
            printf("error\n");
            pthread_exit(NULL);
        }

        // Verificăm dacă mai sunt fișiere de procesat
        if (*thread_args->file_index >= thread_args->files.size()) {
            pthread_mutex_unlock(thread_args->mutex); // Deblocăm mutex-ul
            break;  // Dacă nu mai sunt fișiere, ieșim din buclă
        }

        // Luăm fișierul și actualizăm indexul
        string file = thread_args->files[*thread_args->file_index];
        (*thread_args->file_index)++;  // Incrementăm indexul

        // Afișăm fișierul procesat de thread
        cout << "Thread " << thread_args->thread_id << " processing file: " << file 
             << " (file index: " << *thread_args->file_index << ")" << endl;

        pthread_mutex_unlock(thread_args->mutex); // Deblocăm mutex-ul

        // Aici ar trebui să implementezi logica de procesare a fișierului
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
    
    // thread_args_t thread_args;
    // thread_args.files = files;
    // thread_args.file_index = 0;

    // pthread_t threads[mappers + reducers];
	// int arguments[NUM_THREADS];
	pthread_t *threads = new pthread_t[mappers];  // Array for mappers threads
    thread_args_t *thread_args = new thread_args_t[mappers]; // Dynamically allocated array for thread arguments

    pthread_mutex_t mutex;
    pthread_mutex_init(&mutex, NULL);  // Initialize the mutex

    int file_index = 0;  // Shared file index among threads

    // Create threads for mappers
    for (int i = 0; i < mappers; i++) {
        thread_args[i].files = files;
        thread_args[i].file_index = &file_index; // Share the file_index
        thread_args[i].mutex = &mutex;  // Share the mutex
        thread_args[i].thread_id = i;

        int r = pthread_create(&threads[i], NULL, map_function, &thread_args[i]);
        if (r) {
            printf("Error creating thread %d\n", i);
            exit(-1);
        }
    }


	for (int i = 0; i < mappers; i++) {
		int r = pthread_join(threads[i], NULL);

		if (r) {
			printf("Eroare la asteptarea thread-ului %d\n", i);
			exit(-1);
		}
	}

    cout << *thread_args[0].file_index << endl;
	pthread_mutex_destroy(&mutex);

    // cout << mappers << " " << reducers << endl;
    // for (auto file : files)
    //     cout << file << endl;
   

    return 0;
}