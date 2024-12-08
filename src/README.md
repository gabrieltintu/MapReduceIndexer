### Gabriel-Claudiu TINTU

# Inverted Index using the Map-Reduce Paradigm

- [Description](#description)
- [Input](#input)
- [Map](#map)
- [Reduce](#Reduce)


---

## Description
Parallel program using Pthreads to create an inverted index for a collection of input files. The index contains the list of all unique words from the input files, along with the files in which they are found. 

The program parses the input, then creates and initializes all the mapper and reducer threads. Once the threads are created, they are joined. In the ***func*** function the program checks whether the thread is a mapper. If it is, the thread enters the ***map_function***. If not, it waits at the barrier until all mapper threads have completed their job. After all threads reach the barrier, check whether the thread is a reducer and enter the ***reduce_function*** if it is. Once all thread have finished, the synchronization variables are destroyed and the program ends.

## Input
Start by processing the *args*:

- number of mapper threads;
- number of reducer thread;
- input file listing the paths of all files to be handled

## Map
A mapper thread will process a number of files. For each file opened, the mapper will create a map that will store each unique word along with the ID of the file where it appears. The map created is then added to a shared data structure among all threads (*partial_maps*).

Used a mutex to ensure synchronization during file index updates and when adding data to the shared structure.

## Reduce
A reduce thread will process a subset of maps from the array. It groups the words by their first letter into a local structure, and after completing this,  adds to the *final_maps* array. A mutex ensures synchronization for the addition to the shared array.

Afterward, a barrier synchronizes all reducer threads, making sure the aggregation in *final_maps* is complete. Then, each thread processes a part of *final_maps*. For its assigned letters, it sorts the words accordingly and writes the results to the corresponding file.