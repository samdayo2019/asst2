#include "tasksys.h"
#include <cassert>
#include <iostream>

#ifdef DEBUG
#include <sstream>
#include <stdexcept>
std::mutex cout_mutex; // Mutex for thread-safe printing
// Thread-safe print function
void safe_print(const std::stringstream& message) {
    std::lock_guard<std::mutex> lock(cout_mutex); // Lock the mutex
    std::cout << message.str() << std::endl;      // Thread-safe output
}
#define DCOUT(...)                                                                                 \
    {                                                                                              \
        std::stringstream ss;                                                                      \
        ss << __VA_ARGS__;                                                                         \
        safe_print(ss);                                                                            \
    }
#define DPRINTF(...) printf(__VA_ARGS__)

#define ASSERT(cond)                                                                               \
    {                                                                                              \
        if (!(cond)) {                                                                             \
            throw std::runtime_error("Assertion failed: " #cond);                                  \
        }                                                                                          \
    }

#else
#define DPRINTF(...)
#define DCOUT(...)
#define ASSERT(cond) assert(cond)
#endif

IRunnable::~IRunnable() {}

ITaskSystem::ITaskSystem(int num_threads) {}
ITaskSystem::~ITaskSystem() {}

/*
 * ================================================================
 * Serial task system implementation
 * ================================================================
 */

const char* TaskSystemSerial::name() { return "Serial"; }

TaskSystemSerial::TaskSystemSerial(int num_threads) : ITaskSystem(num_threads) {}

TaskSystemSerial::~TaskSystemSerial() {}

void TaskSystemSerial::run(IRunnable* runnable, int num_total_tasks) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemSerial::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                          const std::vector<TaskID>& deps) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemSerial::sync() { return; }

/*
 * ================================================================
 * Parallel Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelSpawn::name() { return "Parallel + Always Spawn"; }

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads) : ITaskSystem(num_threads) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelSpawn::sync() {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Spinning Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSpinning::name() { return "Parallel + Thread Pool + Spin"; }

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads)
    : ITaskSystem(num_threads) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in
    // Part B.
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in
    // Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable,
                                                              int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in
    // Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in
    // Part B.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Sleeping Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSleeping::name() {
    return "Parallel + Thread Pool + Sleep";
}

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads)
    : ITaskSystem(num_threads) {

    DCOUT("Constructor called");

    this->num_threads = num_threads;

    // Pre-allocate the thread pool so that it won't resize as we push
    threads.reserve(num_threads);
    workers.reserve(num_threads);

    wait_list_handler_thread =
        std::thread(&TaskSystemParallelThreadPoolSleeping::wait_list_handler, this);

    // Each thread will run the `worker_thread` function
    for (int i = 0; i < num_threads; i++) {
        workers.emplace_back(new Worker(i));
        threads.emplace_back(&TaskSystemParallelThreadPoolSleeping::worker_thread, this,
                             workers[i]);

#ifdef PERF
        tasks_per_thread[i] = 0;
#endif
    }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    DCOUT("Destructor called");
    stop = true;
    wait_list_active_signal.notify_one();
    for (int i = 0; i < num_threads; i++) {
        workers[i]->signal.notify_one();
    }

    // Delete all RunInfo objects in run_records
    for (auto& run_record : run_records) {
        delete run_record.second;
    }

    for (Worker* worker : workers) {
        delete worker;
    }

    // Join all threads
    wait_list_handler_thread.join();
    for (std::thread& thread : threads) {
        thread.join();
    }

#ifdef PERF
    // Print out the number of tasks completed by each thread
    // Somehow this prints some threads twice..
    for (auto& pair : tasks_per_thread) {
        std::cout << "Thread " << pair.first << " completed " << pair.second << " tasks"
                  << std::endl;
    }
    printf("\n");
#endif
}

void TaskSystemParallelThreadPoolSleeping::worker_thread(Worker* worker) {
    DCOUT("Worker thread " << worker->id << " started");
    RunInfo* run_info = nullptr;

    while (true) {
        DCOUT("Worker thread " << worker->id << " re-entered");
        TaskInfo task;
        {
            std::unique_lock<std::mutex> lock(worker->mutex);
            worker->signal.wait(lock, [this, &worker] {
                return !worker->queue.empty() || worker->sync_flag || stop;
            });

            DCOUT("Worker thread " << worker->id << " unblocked");

            if (worker->sync_flag && worker->queue.empty()) {
                // Since the sync function can only wait on one lock, we use a separate lock for
                // signaling synced
                {
                    std::lock_guard<std::mutex> lock(workers_sync_mutex);
                    DCOUT("Worker thread " << worker->id << " acknowledged sync signal");
                    worker->sync_flag = false;
                    workers_synced_count.fetch_add(1);
                }
                workers_synced.notify_one();
                continue;
            }

            if (stop) {
                DCOUT("Worker thread " << worker->id << " exiting");
                return;
            }

            ASSERT(!worker->queue.empty());
            task = worker->queue.front();
            worker->queue.pop();
            DCOUT("Worker thread " << worker->id << " got run #" << task.run_id << " " << task.num
                                   << " tasks [" << task.first << ", " << task.last << ", "
                                   << task.step << "]");
        }

        run_info = run_records[task.run_id];
        for (int i = task.first; i <= task.last; i += task.step) {
            run_info->runnable->runTask(i, run_info->num_total_tasks);
        }

        DCOUT("Worker thread " << worker->id << " executed run #" << task.run_id << " " << task.num
                               << " tasks [" << task.first << ", " << task.last << ", " << task.step
                               << "]");

#ifdef PERF
        tasks_per_thread[worker->id] += task.num;
#endif

        // Update the number of completed tasks for this run, if all completed, mark it as done
        // Note here we only need to lock this run entry in the table so that other threads won't
        // write to it at the same time. Insertion to run_records won't affect this pointer
        run_info->run_mutex.lock();
        run_info->num_tasks_completed += task.num;
        if (run_info->num_tasks_completed == run_info->num_total_tasks) {
            DCOUT("Run #" << task.run_id << " completed by worker thread " << worker->id);
            // Mark the run as done
            run_info->is_done = true;
            run_info->run_mutex.unlock();

            // Push the completed run to the action queue and notify wait_list_handler
            {
                std::lock_guard<std::mutex> lock(wait_list_action_mutex);
                DCOUT("Worker thread " << worker->id << " pushed run #" << task.run_id
                                       << " to action queue");
                wait_list_action_queue.push(task.run_id);
            }
            // No need to notify wait_list_signal here because if handler got blocked there it means
            // wait list is empty, so no run can become ready
            wait_list_action_signal.notify_one();
        } else {
            run_info->run_mutex.unlock();
        }
    }
}

/**
 * We want to wake up and check all entries in the wait list as little as possible, only when a run
 * completes or a new run is pushed to the wait list. This means unlike part a we cannot unblock
 * whenever it sees wait list is not empty (which would keep checking all deps even tho nothing has
 * changed).
 If sync_called is true, then we know no more new runs are expectd, but it should not
 * wake up this thread to check deps because we know no dep got completed by that signal. This also
 * means sync_called cannot be a predicate like `stop` in part a. However, we still need to capture
 * the fact that sync_called is set, so from now on whenever the wait list is empty, it means we are
 * done. We achieve this with another block
 */
void TaskSystemParallelThreadPoolSleeping::wait_list_handler(void) {
    DCOUT("Wait list handler started");
    int next_thread_to_enq = 0;

    while (true) {
        {
            // This lock makes sure only this thread is access wait_list
            std::unique_lock<std::mutex> lock(wait_list_mutex);

            // This condition mainly provides a way to know that sync is called so we are ready
            // to wrap up. However, when sync is not yet called we should still allow the thread to
            // listen to actions on the wait list, so we make it transparent when wait list is not
            // empty. This will be fine since before sync is called, this thread always gets blocked
            // here
            // We call it `wait_list_active_list` since it blocks actions when there's nothing in
            // wait list to process
            wait_list_active_signal.wait(
                lock, [this] { return !wait_list.empty() || wait_list_sync_flag || stop; });

            DCOUT("Wait list handler unblocked");

            if (wait_list_sync_flag && wait_list.empty()) {
                DCOUT("Wait list handler received sync signal");
                // Reset the flag
                wait_list_sync_flag = false;
                // Notify sync that wait list is empty
                wait_list_synced.notify_one();
                continue;
            }
        }

        // Because we have sync now, when stop flag is set by destructor, wait list is
        // guaranteed to be empty
        if (stop) {
            DCOUT("Wait list handler exiting");
            return;
        }

        // TODO: I think the next lock doesn't need to be wait_list_mutex since we don't access
        // wait_list itself.
        // We may be able to use another mutex for it

        RunID action_run;

        // Wait for a run to complete so more runs may be ready
        // Two scenarios:
        // (1) When a run is completed, this thread is blocked by wait_list_action_signal, so it
        // gets woken up properly
        // (2) When a run is completed, this thread has not yet been blocked by
        // wait_list_action_signal, so it needs to check whether a completed run is pending.
        // With this predicate added, in case (1) we can still be woken up properly because
        // the action queue should not be empty when the notification is sent.
        // This should only be unblocked when a run is completed - it shouldn't depend on whether
        // wait list is empty, like described in the function description.
        {
            std::unique_lock<std::mutex> lock3(wait_list_action_mutex);
            wait_list_action_signal.wait(lock3, [this, &action_run] {
                // TODO: is this actually needed? Will this ever get woken up if action queue is
                // empty?

                // No pending action, block and wait for new action
                if (wait_list_action_queue.empty()) {
                    DCOUT("Wait list handler gets blocked due to no action");
                    return false;
                } else {
                    //  Get the completd run ID from the action queue
                    action_run = wait_list_action_queue.front();
                    wait_list_action_queue.pop();

                    DCOUT("Wait list handler received action signal: run #" << action_run);
                    return true;
                }
            });
        }

        DCOUT("Wait list size = " << wait_list.size());

        // wait_list is a set, since we know the dep_by of the run just finished, we directly
        // look them up and see if that's ready to go now
        for (RunID dep : run_records[action_run]->dep_by) {
            auto dep_it = run_records.find(dep);
            ASSERT(dep_it != run_records.end());
            DCOUT("Removeing run #" << action_run << " from run #" << dep << " deps");

            // Erase this run from the dep list
            dep_it->second->deps.erase(action_run);
            // If no more dependencies, push the run to the ready queue and remove it from
            // wait list
            if (dep_it->second->deps.empty()) {
                DCOUT("Run #" << dep << " ready to be dispatched from wait list");

                // Same logic as in the run functin. Ideally we use the same `next_thread_to_enq`,
                // but that can cause more contention so we just start from 0. I think the overall
                // performance won't get affected too much unless the workload is of a very specific
                // pattern
                for (int first = 0; first < num_threads; first++) {
                    TaskInfo task(dep_it->first, first, num_threads,
                                  dep_it->second->num_total_tasks);
                    if (task.num == 0) {
                        break;
                    } else {
                        {
                            std::lock_guard<std::mutex> lock(workers[next_thread_to_enq]->mutex);
                            // Put this task in thread `next_thread_to_enq`. This is to prevent the
                            // earlier threads from getting assigned more heavily if we always start
                            // from therad 0
                            workers[next_thread_to_enq]->queue.push(task);
                            DCOUT("Wait list dispatched run #"
                                  << dep_it->first << " " << task.num << " tasks [" << task.first
                                  << ", " << task.last << ", " << task.step
                                  << "] dispatched to worker thread " << next_thread_to_enq);
                        }
                        workers[next_thread_to_enq]->signal.notify_one();
                        next_thread_to_enq = (next_thread_to_enq + 1) % num_threads;
                    }
                }

                // Remove the run from the wait list
                {
                    std::lock_guard<std::mutex> lock(wait_list_mutex);
                    int ret = wait_list.erase(dep_it->first);
                    ASSERT(ret == 1);
                }
            }
        }
    }
}

/**
 * This function is the dispatcher thread that dispatches runs to either ready queue or wait
 * list
 */
TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable,
                                                              int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {

    // Different runs can now process in parallel, so we must record the informatin for each run
    // instead of using shared variables
    RunID run_id = next_run_id++;

    DCOUT("Run #" << run_id << " requested, number of deps = " << deps.size());

    // Record the run information
    RunInfo* run_info = new RunInfo{runnable, num_total_tasks};
    for (RunID dep : deps) {
        RunInfo* dep_info = run_records[dep];

        // TODO: whether we lock each run or not here does not matter since the dep's `is_done`
        // can be set to true before this check is done, so it will be pushed to wait list
        // anyway. So far, seems like that run still eventaully gets processed. Need to think
        // through this, may have some corner cases.
        if (!dep_info->is_done) {
            DCOUT("Run #" << run_id << " dep #" << dep << " not done");
            // Add the dependencies to deps list if they are not done
            run_info->deps.insert(dep);
            // Also add this run to the dep_by of the dependencies so that when they are done we
            // know what blocked runs to check. Need to
            // NOTE: this assumes all the dependeices have to call this function prior to this
            // run so their records are stored
            dep_info->dep_by.insert(run_id);
        } else {
            DCOUT("Run #" << run_id << " dep #" << dep << " is done");
        }
    }

    // Because map insert won't cause rehashing and this is the only thread that would insert
    // entry to run_records, no lock needed
    run_records[run_id] = run_info;
    DCOUT("Run #" << run_id << " recorded");

    int next_thread_to_enq = 0;
    // Divide the run into a number of "bulk tasks" determined by the number of threads and total
    // tasks of the run. Each "bulk task" will be processed by a thread at one time
    // There will be at most `num_threads` bulk tasks per run, but if total number of tasks is less
    // than the available threads, need to create only that many bulk tasks

    // If no deps remaining, push the run to the ready queue, otherwise push it to the wait
    // list
    if (run_info->deps.empty()) {
        for (int first = 0; first < num_threads; first++) {
            TaskInfo task(run_id, first, num_threads, run_info->num_total_tasks);
            if (task.num == 0) {
                break;
            } else {
                {
                    std::lock_guard<std::mutex> lock(workers[next_thread_to_enq]->mutex);
                    // Put this task in thread `next_thread_to_enq`. This is to prevent the earlier
                    // threads from getting assigned more heavily if we always start from therad 0
                    workers[next_thread_to_enq]->queue.push(task);
                    DCOUT("Run #" << task.run_id << " " << task.num << " tasks [" << task.first
                                  << ", " << task.last << ", " << task.step
                                  << "] directly dispatched to worker thread "
                                  << next_thread_to_enq);
                }
                workers[next_thread_to_enq]->signal.notify_one();
                next_thread_to_enq = (next_thread_to_enq + 1) % num_threads;
            }
        }
    } else {
        {
            std::lock_guard<std::mutex> lock(wait_list_mutex);
            wait_list.insert(run_id);
            DCOUT("Run #" << run_id << " pushed to wait list");
        }
        // Tell wait_list_handler that there's new item pushed
        wait_list_active_signal.notify_one();
    }
    return run_id;
}

void TaskSystemParallelThreadPoolSleeping::sync() {
    // Send a sync signal to tell handler thrads that no more runs are expected
    // When wait_list_active_signal completes predicate check (evaluate to false) and before it
    // enters waiting state, if this function is called and modifies sync_called and notifies,
    // then the notification is missed and predicate won't ever be evaluated again. So we need
    // to lock here.

    {
        std::lock_guard<std::mutex> lock(wait_list_mutex);
        wait_list_sync_flag = true;
        DCOUT("Wait list sync called");
    }
    wait_list_active_signal.notify_one();

    {
        std::unique_lock<std::mutex> lock(wait_list_mutex);
        wait_list_synced.wait(lock, [this] { return !wait_list_sync_flag; });
        // wait_list_sync_flag = false;
        DCOUT("Wait list synced");
    }

    for (int i = 0; i < num_threads; i++) {
        {
            std::lock_guard<std::mutex> lock(workers[i]->mutex);
            workers[i]->sync_flag = true;
            DCOUT("Task queue " << i << " sync called");
        }
        workers[i]->signal.notify_one();
    }

    {
        std::unique_lock<std::mutex> lock(workers_sync_mutex);
        workers_synced.wait(lock, [this] { return workers_synced_count == num_threads; });
        workers_synced_count = 0;
        DCOUT("Task queue synced");
    }

    DPRINTF("Number of items left in action queue: %lu\n", wait_list_action_queue.size());

    // Actually, not clearing won't cause functional error, it's just that the next time wait
    // list checks on action it may waste some cycles Clear wait list action queue so it doesn't
    // affect the next run Don't need to lock here since no contention after sync is called
    // while (!wait_list_action_queue.empty()) {
    //     wait_list_action_queue.pop();
    // }

#ifdef DEBUG
#ifdef PERF
    // Print out the number of tasks completed by each thread
    // Somehow this prints some threads twice..
    for (auto& pair : tasks_per_thread) {
        std::cout << "Thread " << pair.first << " completed " << pair.second << " tasks"
                  << std::endl;
        pair.second = 0;
    }
    printf("\n");
#endif
#endif
}

// TODO: looks like I need to implement this for part b as well
void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {
    runAsyncWithDeps(runnable, num_total_tasks, {});
    sync();
}
