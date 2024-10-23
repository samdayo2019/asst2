#include "tasksys.h"
#include <cassert>

#ifdef DEBUG
#include <iostream>
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

    // Pre-allocate the thread pool so that it won't resize as we push
    thread_pool.reserve(num_threads);
    // task_queues.resize(num_threads);

    wait_list_handler_thread =
        std::thread(&TaskSystemParallelThreadPoolSleeping::wait_list_handler, this);

    // Each thread will run the `thread_worker` function
    for (int i = 0; i < num_threads; i++) {
        thread_pool.emplace_back(&TaskSystemParallelThreadPoolSleeping::worker_thread, this, i);
#ifdef DEBUG
        tasks_per_thread[i] = 0;
#endif
    }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    DCOUT("Destructor called");
    stop = true;
    wait_list_active_signal.notify_one();
    worker_signal.notify_all();

    // Delete all RunInfo objects in run_records
    for (auto& run_record : run_records) {
        delete run_record.second;
    }

    // Join all threads
    wait_list_handler_thread.join();
    for (std::thread& thread : thread_pool) {
        thread.join();
    }
}

void TaskSystemParallelThreadPoolSleeping::worker_thread(int worker_id) {
    DCOUT("Worker thread " << worker_id << " started");
    RunID run_id = -1;
    RunInfo* run_info = nullptr;

    while (true) {
        std::pair<RunID, int> task;
        {
            std::unique_lock<std::mutex> lock(task_queue_mutex);
            worker_signal.wait(
                lock, [this] { return !task_queue.empty() || task_queue_sync_flag || stop; });

            if (task_queue_sync_flag && task_queue.empty()) {
                DCOUT("Worker thread " << worker_id << " received sync signal");

                // After the last task is popped from queue by a thread, that thread may still be
                // working on it when this thread gets here, but the task is not officially done.
                // We check run_records to see if all runs are done
                // TODO: there may be a more efficient way to do this

                // Don't think I need to lock it here since it won't get pushed after sync is called
                bool all_runs_done = true;
                for (auto& run_record : run_records) {
                    if (run_record.second->is_done == false) {
                        all_runs_done = false;
                        break;
                    }
                }
                // If not all runs are done, we put this thread to sleep
                if (!all_runs_done) {
                    DCOUT("Worker thread " << worker_id << " put to sleep, waiting for sync");
                    task_queue_synced.wait(lock, [this] { return !task_queue_sync_flag; });
                    DCOUT("Worker thread " << worker_id << " woke up after sync");
                    continue;
                } else {
                    // All runs are done, acknowledge the sync signal
                    DCOUT("Worker thread " << worker_id << " acknowledges sync signal");
                    task_queue_sync_flag = false;
                    // This notifies both sync function and all other worker threads
                    task_queue_synced.notify_all();

                    continue;
                }
            }

            if (stop) {
                DCOUT("Worker thread " << worker_id << " exiting");
                return;
            }

            // task queue may be empty if sync is called, and the final task has been pop by another
            // thread but it's still working on it
            if (!task_queue.empty()) {
                task = task_queue.front();
                task_queue.pop();
                DCOUT("Worker thread " << worker_id << " got run #" << task.first << " task #"
                                       << task.second);

            } else {
                // TODO: this will cause busy wait until sync flag is unset. It occurs only at the
                // very end of a sync call so it's infrequent
                DCOUT("Worker thread " << worker_id << " woke up but no task to run");
                continue;
            }
        }

        // Save both the run_id and run_info, compare to last run_id, if same then avoid lookup
        if (run_id == task.first) {
            // Run the task
            run_info->runnable->runTask(task.second, run_info->num_total_tasks);
        } else {
            run_id = task.first;
            // Find the run info from the run_records table
            {
                std::lock_guard<std::mutex> lock(run_records_mutex);
                auto run_it = run_records.find(task.first);
                ASSERT(run_it != run_records.end());
                run_info = run_it->second;
            }
            // Run the task
            run_info->runnable->runTask(task.second, run_info->num_total_tasks);
        }

#ifdef DEBUG
        tasks_per_thread[worker_id]++;
#endif

        // Update the number of completed tasks for this run, if all completed, mark it as done
        // Note here we only need to lock this run entry in the table so that other threads won't
        // write to it at the same time
        // I think insertion to run_records won't affect this pointer
        run_info->run_mutex.lock();
        // TODO: change this to atomic and see if it improves performance
        ++run_info->num_tasks_completed;
        if (run_info->num_tasks_completed == run_info->num_total_tasks) {
            DCOUT("Run #" << task.first << " completed by worker thread " << worker_id);
            // Mark the run as done
            run_info->is_done = true;
            run_info->run_mutex.unlock();

            // Push the completed run to the action queue and notify wait_list_handler
            {
                std::lock_guard<std::mutex> lock(wait_list_action_mutex);
                DCOUT("Worker thread " << worker_id << " pushed run #" << task.first
                                       << " to action queue");
                wait_list_action_queue.push(task.first);
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

                    DCOUT("Wait list handler received action signal: run " << action_run
                                                                           << " completed");
                    return true;
                }
            });
        }

        std::vector<RunID> became_ready;

        // Because we must look up the run record for each dep_by, we lock the whole run_records for
        // the entire loop
        // TODO: need to figure out how to not lock the entire run_records
        run_records_mutex.lock();
        // wait_list is a set, since we know the dep_by of the run just finished, we directly
        // look them up and see if that's ready to go now
        for (RunID dep : run_records[action_run]->dep_by) {
            auto dep_it = run_records.find(dep);
            ASSERT(dep_it != run_records.end());

            // Erase this run from the dep list
            dep_it->second->deps.erase(action_run);
            // If no more dependencies, push the run to the ready queue and remove it from
            // wait list
            if (dep_it->second->deps.empty()) {
                became_ready.push_back(dep);
            }
        }
        run_records_mutex.unlock();

        for (RunID run : became_ready) {
            {
                std::lock_guard<std::mutex> lock(task_queue_mutex);
                run_records_mutex.lock();
                auto run_it = run_records.find(run);
                ASSERT(run_it != run_records.end());
                RunInfo* run_info = run_it->second;
                run_records_mutex.unlock();

                for (int i = 0; i < run_info->num_total_tasks; i++) {
                    task_queue.push(std::make_pair(run, i));
                }
                DCOUT("Run #" << run << " moved from wait list to task queue");
            }
            // Notify threads that there are tasks to run
            worker_signal.notify_all();
            {
                std::lock_guard<std::mutex> lock(wait_list_mutex);
                int ret = wait_list.erase(run);
                ASSERT(ret == 1);
            }
        }
    }
}

/**
 * This function is the dispatcher thread that dispatches runs to either task queue or wait
 * list
 */
TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable,
                                                              int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {

    // Different runs can now process in parallel, so we must record the informatin for each run
    // instead of using shared variables
    RunID run_id = next_run_id++;

    DCOUT("Run #" << run_id << " requested");

    // Record the run information
    RunInfo* run_info = new RunInfo{runnable, num_total_tasks};
    for (RunID dep : deps) {
        // Add the dependencies to deps list if they are not done
        // Also add this run to the dep_by of the dependencies so that when they are done we
        // know what blocked runs to check
        if (!run_records[dep]->is_done) {
            run_info->deps.insert(dep);
            // NOTE: this assumes all the dependeices have to call this function prior to this
            // run so their records are stored
            run_records[dep]->dep_by.insert(run_id);
        }
    }

    // Lock the whole run_records to insert the new run because it may cause rehashing
    {
        std::lock_guard<std::mutex> lock(run_records_mutex);
        run_records[run_id] = run_info;
    }

    DCOUT("Run #" << run_id << " recorded");

    // If no deps remaining, push the run to the task queue, otherwise push it to the wait list
    if (run_info->deps.empty()) {
        {
            std::lock_guard<std::mutex> lock(task_queue_mutex);
            for (int i = 0; i < run_info->num_total_tasks; i++) {
                task_queue.push(std::make_pair(run_id, i));
            }
            DCOUT("Run #" << run_id << " pushed to task queue");
        }
        worker_signal.notify_all();
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

    {
        std::unique_lock<std::mutex> lock(task_queue_mutex);
        task_queue_sync_flag = true;
        DCOUT("Task queue sync called");
    }
    worker_signal.notify_all();

    {
        std::unique_lock<std::mutex> lock(task_queue_mutex);
        task_queue_synced.wait(lock, [this] { return !task_queue_sync_flag; });
        DCOUT("Task queue synced");
    }

    DPRINTF("Number of items left in action queue: %lu\n", wait_list_action_queue.size());

    // Actually, not clearing won't cause functional error, it's just that the next time wait list
    // checks on action it may waste some cycles
    // Clear wait list action queue so it doesn't affect the next run
    // Don't need to lock here since no contention after sync is called
    // while (!wait_list_action_queue.empty()) {
    //     wait_list_action_queue.pop();
    // }

#ifdef DEBUG
    // Print out the number of tasks completed by each thread
    // Somehow this prints some threads twice..
    for (auto& pair : tasks_per_thread) {
        std::cout << "Thread " << pair.first << " completed " << pair.second << " tasks"
                  << std::endl;
        pair.second = 0;
    }
    printf("\n");
#endif
}

// TODO: looks like I need to implement this for part b as well
void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {
    runAsyncWithDeps(runnable, num_total_tasks, {});
    sync();
}
