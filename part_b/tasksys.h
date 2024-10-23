#ifndef _TASKSYS_H
#define _TASKSYS_H

#include "itasksys.h"
#include <atomic>
#include <condition_variable>
#include <mutex>
#include <queue>
#include <thread>
#include <unordered_map>
#include <unordered_set>
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
        std::unordered_set<RunID> dep_by; // depended by
        std::unordered_set<RunID> deps;   // depending on
        int num_tasks_completed = 0;
        bool is_done = false;
        std::mutex run_mutex;

        RunInfo(IRunnable* runnable, int num_total_tasks)
            : runnable(runnable), num_total_tasks(num_total_tasks) {}
    };
    std::unordered_map<RunID, RunInfo*> run_records; // lookup table for run information
    std::mutex run_records_mutex;                    // lock for the entire run_records

    RunID next_run_id = 0;
    std::vector<std::thread> thread_pool;
    std::queue<std::pair<RunID, int>> task_queue; // (run_id, task_id)
    std::mutex task_queue_mutex;
    std::condition_variable worker_signal; // signal to wake up worker thread
    // std::vector<std::queue<TaskInfo>> task_queues;
    std::unordered_set<RunID> wait_list;
    std::mutex wait_list_mutex;
    void wait_list_handler(void);
    std::thread wait_list_handler_thread;
    void worker_thread(int worker_id);
    std::condition_variable wait_list_empty_signal;
    std::queue<RunID> wait_list_action_queue;
    std::mutex wait_list_action_mutex;
    std::condition_variable wait_list_action_signal;
    std::condition_variable wait_list_active_signal;
    bool wait_list_sync_flag = false;
    bool task_queue_sync_flag = false;
    std::mutex sync_mutex;
    std::condition_variable wait_list_synced, task_queue_synced;
    std::condition_variable sync_completed;
    bool stop = 0;
#ifdef DEBUG
    // the number of tasks each thread has completed
    std::unordered_map<int, int> tasks_per_thread;
#endif
};

#endif
