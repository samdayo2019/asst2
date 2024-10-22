#include "tasksys.h"

#ifdef DEBUG
#include <cassert>
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
#define ASSERT(cond)
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

    ready_queue_handler_thread =
        std::thread(&TaskSystemParallelThreadPoolSleeping::ready_queue_handler, this);
    wait_list_handler_thread =
        std::thread(&TaskSystemParallelThreadPoolSleeping::wait_list_handler, this);

    // Each thread will run the `thread_worker` function
    for (int i = 0; i < num_threads; i++) {
        thread_pool.emplace_back(&TaskSystemParallelThreadPoolSleeping::worker_thread, this, i);
    }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    DCOUT("Destructor called");
    // task_queue_mutex.lock();
    // wait_list_mutex.lock();
    // ready_queue_mutex.lock();
    stop = true;
    // task_queue_mutex.unlock();
    // wait_list_mutex.unlock();
    // ready_queue_mutex.unlock();
    wait_list_active_signal.notify_one();
    ready_queue_signal.notify_one();
    worker_signal.notify_all();

    // Delete all RunInfo objects in run_records
    for (auto& run_record : run_records) {
        delete run_record.second;
    }

    // Join all threads
    ready_queue_handler_thread.join();
    wait_list_handler_thread.join();
    for (std::thread& thread : thread_pool) {
        thread.join();
    }
}

void TaskSystemParallelThreadPoolSleeping::worker_thread(int worker_id) {
    DCOUT("Worker thread " << worker_id << " started");

    while (true) {
        std::pair<RunID, int> task;
        {
            std::unique_lock<std::mutex> lock(task_queue_mutex);
            worker_signal.wait(
                lock, [this] { return !task_queue.empty() || task_queue_sync_flag || stop; });

            // NOTE: checking sync flag is put here because we must wait until the final task
            // completes. It's inside this if body so this check occurs less often, since we know
            // the last task must complete a run.
            // Since at this point wait list is already synced, further runs in action queue are
            // useless (nothing depends on them). Need to remember clear it in sync so it doesn't
            // affect the next run
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

        // Find the run info from the run_records table, save it to local variable so we can release
        RunInfo* run_info;
        {
            std::lock_guard<std::mutex> lock(run_records_mutex);
            auto run_it = run_records.find(task.first);
            ASSERT(run_it != run_records.end());
            run_info = run_it->second;
        }

        // Run the task
        run_info->runnable->runTask(task.second, run_info->num_total_tasks);

        int num_tasks_completed;
        // Update the number of completed tasks for this run, if all tasks are completed, mark it
        // as done
        // Note here we only need to lock this run entry in the table so that other threads won't
        // access it at the same time I think insertion to run_records won't affect this pointer
        {
            std::lock_guard<std::mutex> lock(run_info->run_mutex);
            // Save to local variable so we can release lock earlier
            // TODO: change this to atomic and see if it improves performance
            num_tasks_completed = ++run_info->num_tasks_completed;
        }

        if (num_tasks_completed == run_info->num_total_tasks) {
            DCOUT("Run #" << task.first << " completed by worker thread " << worker_id);
            // Mark the run as done
            {
                std::lock_guard<std::mutex> lock(run_info->run_mutex);
                run_info->is_done = true;
            }

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
        }
    }
}

/**
 * Ready queue is more like the worker thread style because it can always try to empty the queue
 */
void TaskSystemParallelThreadPoolSleeping::ready_queue_handler() {
    DCOUT("Ready queue handler started");
    while (true) {
        RunID run_id;
        {
            std::unique_lock<std::mutex> lock(ready_queue_mutex);
            ready_queue_signal.wait(
                lock, [this] { return !ready_queue.empty() || ready_queue_sync_flag || stop; });

            if (ready_queue_sync_flag && ready_queue.empty()) {
                DCOUT("Ready queue handler received sync signal");
                // Reset the flag
                // {
                //     std::unique_lock<std::mutex> lock(sync_mutex);
                ready_queue_sync_flag = false;
                // }
                // Notify sync that ready queue is empty
                ready_queue_synced.notify_one();
                continue;
            }

            if (stop) {
                DCOUT("Ready queue handler exiting");
                return;
            }

            ASSERT(!ready_queue.empty());

            // Get the next run from the ready queue and dispatch tasks
            // TODO: may be able to improve by emptying ready_queue in each go
            run_id = ready_queue.front();
            ready_queue.pop();
        }

        // Dispatch tasks of the run to task queue
        {
            std::lock_guard<std::mutex> lock(task_queue_mutex);
            run_records_mutex.lock();
            RunInfo* run_info = run_records[run_id];
            run_records_mutex.unlock();

            for (int i = 0; i < run_info->num_total_tasks; i++) {
                task_queue.push(std::make_pair(run_id, i));
            }
            DCOUT("Run #" << run_id << " dispatched to task queue");
        }
        // Notify threads that there are tasks to run
        worker_signal.notify_all();
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

        // TODO: need to figure out how to not lock the entire run_records
        run_records_mutex.lock();

        // TODO: if this holds true then we don't have to reset action_run
        ASSERT(action_run != -1);

        // wait_list is a set, since we know the dep_by of the run just finished, we directly
        // look them up and see if that's ready to go now
        for (RunID dep : run_records[action_run]->dep_by) {
            // Erase this run from the dep list
            run_records[dep]->deps.erase(action_run);
            // If no more dependencies, push the run to the ready queue and remove it from
            // wait list
            if (run_records[dep]->deps.empty()) {
                {
                    std::lock_guard<std::mutex> lock(ready_queue_mutex);
                    ready_queue.push(dep);
                }
                DCOUT("Run #" << dep << " moved from wait list to ready queue");
                // Notify ready_queue_handler that there is a new run to process
                ready_queue_signal.notify_one();
                {
                    std::lock_guard<std::mutex> lock(wait_list_mutex);
                    wait_list.erase(dep);
                }
            }
        }
        run_records_mutex.unlock();
        action_run = -1;
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

    DCOUT("Run #" << run_id << " requested");

    // Record the run information
    RunInfo* run_info = new RunInfo{runnable, num_total_tasks};
    for (RunID dep : deps) {
        // Add the dependencies to deps list if they are not done
        // Also add this run to the dep_by of the dependencies so that when they are done we
        // know what blocked runs to check
        // This requires the run_records to be locked
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
        for (RunID dep : deps) {
            // Add the dependencies to deps list if they are not done
            // Also add this run to the dep_by of the dependencies so that when they are done we
            // know what blocked runs to check
            // This requires the run_records to be locked
            // TODO: technically we don't have to lock the whole map
            if (!run_records[dep]->is_done) {
                run_info->deps.insert(dep);
                // NOTE: this assumes all the dependeices have to call this function prior to this
                // run so their records are stored
                run_records[dep]->dep_by.insert(run_id);
            }
        }
        run_records[run_id] = run_info;
    }

    DCOUT("Run #" << run_id << " recorded");

    // If no deps remaining, push the run to the ready queue, otherwise push it to the wait list
    if (run_info->deps.empty()) {
        {
            std::lock_guard<std::mutex> lock(ready_queue_mutex);
            ready_queue.push(run_id);
            DCOUT("Run #" << run_id << " pushed to ready queue");
        }
        // Tell ready_queue_handler that there is a new run to process
        ready_queue_signal.notify_one();
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
        std::unique_lock<std::mutex> lock(ready_queue_mutex);
        ready_queue_sync_flag = true;
        DCOUT("Ready queue sync called");
    }
    ready_queue_signal.notify_one();

    {
        std::unique_lock<std::mutex> lock(ready_queue_mutex);
        ready_queue_synced.wait(lock, [this] { return !ready_queue_sync_flag; });
        DCOUT("Ready queue synced");
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

    // Clear wait list action queue so it doesn't affect the next run
    {
        std::lock_guard<std::mutex> lock(wait_list_action_mutex);
        while (!wait_list_action_queue.empty()) {
            wait_list_action_queue.pop();
        }
    }
    DCOUT("Worker threads sync called");
}

// TODO: looks like I need to implement this for part b as well
void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {
    runAsyncWithDeps(runnable, num_total_tasks, {});
    sync();
}
