package mapreduce

import (
	"encoding/json"
	"os"
	"sort"
)

// doReduce manages one reduce task: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.
func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// You will need to write this function.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTaskNumber) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//

	keyValues := make(map[string][]string, 0)
	//Read the intermediate file
	for i := 0; i < nMap; i++ {
		filename := reduceName(jobName, i, reduceTaskNumber)
		file, _ := os.OpenFile(filename, os.O_RDWR, 0666)
		dec := json.NewDecoder(file)
		for {
			var v KeyValue
			err := dec.Decode(&v)
			if err != nil {
				break
			}
			_, ok := keyValues[v.Key]
			if !ok {
				keyValues[v.Key] = make([]string, 0)
			}
			keyValues[v.Key] = append(keyValues[v.Key], v.Value)
		}
		file.Close()
	}

	var keys []string
	for k, _ := range keyValues {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	mergeFileName := mergeName(jobName, reduceTaskNumber)
	mergeFile, err := os.Create(mergeFileName)
	if err != nil {
		panic(err)
	}
	defer mergeFile.Close()

	enc := json.NewEncoder(mergeFile)
	for _, key := range keys {
		res := reduceF(key, keyValues[key])
		err := enc.Encode(&KeyValue{key, res})
		if err != nil {
			panic(err)
		}
	}

	//sort.Sort(ByAgepeople))

}
