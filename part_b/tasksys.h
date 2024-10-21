#ifndef _TASKSYS_H
#define _TASKSYS_H

#include "itasksys.h"
#include <condition_variable>
#include <atomic>
#include <mutex>
#include <queue>
#include <thread>
#include <unordered_map>
#include <vector>

/*
 * TaskSystemSerial: This class is the student's implementation of a
 * serial task execution engine.  See definition of ITaskSystem in
 * itasksys.h for documentation of the ITaskSystem interface.
 */
class TaskSystemSerial : public ITaskSystem {
  public:
    TaskSystemSerial(int num_threads);
    ~TaskSystemSerial();
    const char* name();
    void run(IRunnable* runnable, int num_total_tasks);
    TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                            const std::vector<TaskID>& deps);
    void sync();
};

/*
 * TaskSystemParallelSpawn: This class is the student's implementation of a
 * parallel task execution engine that spawns threads in every run()
 * call.  See definition of ITaskSystem in itasksys.h for documentation
 * of the ITaskSystem interface.
 */
class TaskSystemParallelSpawn : public ITaskSystem {
  public:
    TaskSystemParallelSpawn(int num_threads);
    ~TaskSystemParallelSpawn();
    const char* name();
    void run(IRunnable* runnable, int num_total_tasks);
    TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                            const std::vector<TaskID>& deps);
    void sync();
};

/*
 * TaskSystemParallelThreadPoolSpinning: This class is the student's
 * implementation of a parallel task execution engine that uses a
 * thread pool. See definition of ITaskSystem in itasksys.h for
 * documentation of the ITaskSystem interface.
 */
class TaskSystemParallelThreadPoolSpinning : public ITaskSystem {
  public:
    TaskSystemParallelThreadPoolSpinning(int num_threads);
    ~TaskSystemParallelThreadPoolSpinning();
    const char* name();
    void run(IRunnable* runnable, int num_total_tasks);
    TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                            const std::vector<TaskID>& deps);
    void sync();
};

/*
 * TaskSystemParallelThreadPoolSleeping: This class is the student's
 * optimized implementation of a parallel task execution engine that uses
 * a thread pool. See definition of ITaskSystem in
 * itasksys.h for documentation of the ITaskSystem interface.
 */
class TaskSystemParallelThreadPoolSleeping : public ITaskSystem {
  public:
    TaskSystemParallelThreadPoolSleeping(int num_threads);
    ~TaskSystemParallelThreadPoolSleeping();
    const char* name();
    void run(IRunnable* runnable, int num_total_tasks);
    TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                            const std::vector<TaskID>& deps);
    void sync();

  private:
    // TaskID is really the run id, but was named "TaskID" in tests.h
    typedef TaskID RunID;
    struct RunInfo {
        IRunnable* runnable;
        int num_total_tasks;
        std::vector<TaskID> deps;
        int num_tasks_completed = 0;
        bool is_done = false;
        std::mutex run_mutex;
        
        RunInfo(IRunnable* runnable, int num_total_tasks, const std::vector<TaskID>& deps)
            : runnable(runnable), num_total_tasks(num_total_tasks), deps(deps) {}
    };
    std::unordered_map<RunID, RunInfo*> run_records; // lookup table for run information
    std::mutex run_records_mutex; // lock for the entire run_records

    RunID next_run_id = 0;
    std::vector<std::thread> thread_pool;
    std::queue<std::pair<RunID, int>> task_queue; // (run_id, task_id)
    std::mutex task_queue_mutex;
    std::condition_variable worker_signal; // signal to wake up worker thread
    bool sync_workers = false;
    // std::vector<std::queue<TaskInfo>> task_queues;
    std::queue<RunID> ready_queue;
    std::mutex ready_queue_mutex;
    std::condition_variable ready_queue_signal;
    void ready_queue_handler(void);
    std::thread ready_queue_handler_thread;
    std::vector<RunID> wait_list;
    std::mutex wait_list_mutex;
    void wait_list_handler(void);
    std::thread wait_list_handler_thread;
    void worker_thread(int worker_id);
    std::condition_variable wait_list_empty_signal;
    std::condition_variable ready_queue_empty_signal;
    // -1 means new run pushed to wait_list, non-neg numbers are run_id
    std::queue<RunID> wait_list_action_queue;
    std::mutex wait_list_action_mutex;
    std::condition_variable wait_list_action_signal;
    std::condition_variable wait_list_active_signal;
    // Separate sync flags for wait_list and ready_queue, because we need to first make sure wait
    // list is emptied, then can check if ready queue is emptied
    bool sync_wait_list = false, sync_ready_queue = false;
};

#endif
