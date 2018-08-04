package mapreduce

import (
	"fmt"
	"sync"
)

//
// schedule() starts and waits for all tasks in the given phase (Map
// or Reduce). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)
	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//
	//1.Get available work from register channle (<-registerChan)
	//2.To use DoTask service for each task
	//3.Register the worker to master after task done
	//4.Do not return until all the tasks finished
	var wg sync.WaitGroup
	for task := 0; task < ntasks; task++ {
		taskArgs := new(DoTaskArgs)
		taskArgs.JobName = jobName
		taskArgs.TaskNumber = task
		taskArgs.NumOtherPhase = n_other
		taskArgs.Phase = phase
		if phase == mapPhase{
			taskArgs.File = mapFiles[task]
		} else {
			taskArgs.File = ""
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
		
		CallDoTask:
			worker := <-registerChan
			taskState := call(worker, "Worker.DoTask", taskArgs, new(struct{}))
			if taskState == false {
				fmt.Printf("Worker: RPC %s DoTask error\n", worker)
				go func() { registerChan <- worker }()
				goto CallDoTask
			}
			//Release worker to registerChan
			go func() { registerChan <- worker }()
		}()

	}
	//hand out task to the work
	wg.Wait()
	fmt.Printf("Schedule: %v phase done\n", phase)
}