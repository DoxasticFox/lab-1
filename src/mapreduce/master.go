package mapreduce
import "container/list"
import "fmt"
import "sync"		// It didn't have to be this way...

type WorkerInfo struct {
  address string
  // You can add definitions here.
	failed bool
}


// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
  l := list.New()
  for _, w := range mr.Workers {
    DPrintf("DoWork: shutdown %s\n", w.address)
    args := &ShutdownArgs{}
    var reply ShutdownReply;
    ok := call(w.address, "Worker.Shutdown", args, &reply)
    if ok == false {
      fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
    } else {
      l.PushBack(reply.Njobs)
    }
  }
  return l
}

// Distributes jobs to a set of workers
func (mr *MapReduce) RunMaster() *list.List {
	mr.Workers = make(map[string]*WorkerInfo)
	mr.WorkersMtx = &sync.Mutex{}

	/* incessantly listen for new workers and add them to mr.Workers */
	go func() {
		for {
			var wi WorkerInfo
			wi.address = <-mr.registerChannel

			mr.WorkersMtx.Lock()
			mr.Workers[wi.address] = &wi
			mr.WorkersMtx.Unlock()
		}
	}()

	/* Map me... */
	for i := 0; i < mr.nMap; {
		mr.WorkersMtx.Lock()
		// try assigning work
		for _, w := range mr.Workers {
			if w.failed { continue }
		  DPrintf("DoWork: job %s\n", w.address)
		  args := &DoJobArgs{mr.file, "Map", i, mr.nReduce}
		  var reply DoJobReply;
		  ok := call(w.address, "Worker.DoJob", args, &reply)
		  if ok == false {
				w.failed = true
		    fmt.Printf("DoWork: RPC %s job error\n", w.address)
		  } else {
				i += 1
			}
			if i >= mr.nMap {
				break
			}
		}
		mr.WorkersMtx.Unlock()
	}

	/* ...and then reduce me... */
	for i := 0; i < mr.nReduce; {
		mr.WorkersMtx.Lock()
		// try assigning work
		for _, w := range mr.Workers {
			if w.failed { continue }
		  DPrintf("DoWork: job %s\n", w.address)
		  args := &DoJobArgs{mr.file, "Reduce", i, mr.nMap}
		  var reply DoJobReply;
		  ok := call(w.address, "Worker.DoJob", args, &reply)
		  if ok == false {
				w.failed = true
		    fmt.Printf("DoWork: RPC %s job error\n", w.address)
		  } else {
				i += 1
			}
			if i >= mr.nReduce {
				break
			}
		}
		mr.WorkersMtx.Unlock()
	}
	/* ...until I can get my... */
  return mr.KillWorkers() // ...termination.
}
