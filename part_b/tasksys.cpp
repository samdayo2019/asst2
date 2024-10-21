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

    // task_queues[i] is the queue of tasks for thread thread_pool[i]

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
    // Delete all RunInfo objects in run_records
    for (auto& run_record : run_records) {
        delete run_record.second;
    }


  // FIX: these are here to temporarily fix `run` function since it's not implemented yet
    {
        std::lock_guard<std::mutex> lock(wait_list_mutex);
        sync_wait_list = true;
    }
    wait_list_active_signal.notify_one();

    // Must make sure wait list is emptied before syncing the ready queue
    if (wait_list_handler_thread.joinable())
        wait_list_handler_thread.join();

    {
        std::lock_guard<std::mutex> lock(ready_queue_mutex);
        sync_ready_queue = true;
    }
    ready_queue_signal.notify_one();

    if (ready_queue_handler_thread.joinable())
        ready_queue_handler_thread.join();

    // Now we can sync all worker threads
    {
        std::lock_guard<std::mutex> lock(task_queue_mutex);
        sync_workers = true;
    }
    worker_signal.notify_all();

    // Join all threads in the thread pool
    for (std::thread& thread : thread_pool) {
        if (thread.joinable())
            thread.join();
    }
}

void TaskSystemParallelThreadPoolSleeping::worker_thread(int worker_id) {
    DCOUT("Worker thread " << worker_id << " started");

    while (true) {
        std::pair<RunID, int> task;
        {
            std::unique_lock<std::mutex> lock(task_queue_mutex);
            worker_signal.wait(lock, [this] { return !task_queue.empty() || sync_workers; });

            if (sync_workers && task_queue.empty()) {
                return;
            }

            ASSERT(!task_queue.empty());

            task = task_queue.front();
            task_queue.pop();
        }

        // Run the task
        run_records_mutex.lock();
        auto run_it = run_records.find(task.first);
        ASSERT(run_it != run_records.end());
        run_it->second->runnable->runTask(task.second, run_it->second->num_total_tasks);
        run_records_mutex.unlock();

        bool is_done = false;
        // Save to local variable so we can release lock earlier
        int num_tasks_completed, num_total_tasks;
        {
            // Lock the run record to update the number of completed tasks for this run
            // std::lock_guard<std::mutex> lock(run_it->second->run_mutex);
            std::lock_guard<std::mutex> lock(run_records_mutex);
            // TODO: try convert this to atomic and see how it impacts performance
            num_tasks_completed = ++run_it->second->num_tasks_completed;
            num_total_tasks = run_it->second->num_total_tasks;
        }

        // Because increment and comparison are put in the same critical section, only one
        // thread can evalute to true here
        if (num_tasks_completed == num_total_tasks) {
            // Set a local variable for sending notification outside the critical section
            // This is a solution to the problem described in part a
            is_done = true;
            // Save the run id of the run just completed to wait list action
            // Lock it here so other threads cannot overwrite it before it gets processed
            // wait_list_handler is responsible for unlocking it
            {
                std::lock_guard<std::mutex> lock(wait_list_action_mutex);
                DCOUT("Worker thread " << worker_id << " pushed run #" << task.first
                                       << " to action queue");
                wait_list_action_queue.push(task.first);
            }
        }

        // Convince yourself: no need to notify wait_list_signal here because that would mean no run
        // in wait list, so this signal doesn't matter
        if (is_done)
            wait_list_action_signal.notify_one();
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
            ready_queue_signal.wait(lock,
                                    [this] { return !ready_queue.empty() || sync_ready_queue; });

            if (sync_ready_queue && ready_queue.empty()) {
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
            auto run_it = run_records.find(run_id);
            ASSERT(run_it != run_records.end());
            for (int i = 0; i < run_it->second->num_total_tasks; i++) {
                task_queue.push(std::make_pair(run_id, i));
            }
            run_records_mutex.unlock();
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

            DCOUT("Wait list handler re-entered");
            // This condition mainly provides a way to know that sync is called so we are ready
            // to wrap up. However, when sync is not yet called we should still allow the thread to
            // listen to actions on the wait list, so we make it transparent when wait list is not
            // empty. This will be fine since before sync is called, this thread always gets blocked
            // here
            // We call it `wait_list_active_list` since it blocks actions when there's nothing in
            // wait list to process
            wait_list_active_signal.wait(lock,
                                         [this] { return !wait_list.empty() || sync_wait_list; });

            DCOUT("Wait list handler unblocked");

            // if sync is signaled and wait list is empty, meaning all runs have been pushed to
            // ready queue, and this thread is done
            if (wait_list.empty() && sync_wait_list) {
                DCOUT("Wait list handler synced");
                return;
            }

            ASSERT(!wait_list.empty());

            // TODO: I think the next lock doesn't need to be wait_list_mutex since we don't access
            // wait_list itself.
            // We may be able to use another mutex for it

            // Wait for a run to complete so more runs may be ready
            // Two scenarios:
            // (1) When a run is completed, this thread is blocked by run_completed_signal, so it
            // gets woken up properly
            // (2) When a run is completed, this thread has not yet been blocked by
            // run_completed_signal, so it needs to check whether a completed run is pending.
            // Now with this predicate added, in case (1) we can still be woken up properly because
            // `what_is_done` should have been set when the notification is sent
            // This should only be woken up when a run is completed. It shouldn't skip wait even
            // if wait list is not empty, otherwise this won't block at all. In worker thread,
            // it sets the latest completed run in `what_is_done` and notify. This thread is
            // responsible for recording the run is done in `run_records`. But at this wait,
            // `run_records[what_is_done].is_done` hasn't been updated yet. So even if we missed
            // the notification, we need to check if `run_records[what_is_done]`.
            wait_list_action_signal.wait(lock, [this] {
                // Note here we are not checking on the lock because upon receiving the notification
                // `what_is_done` is locked, and this thread unlocks it Meaning there's pending run
                // to be processed
                wait_list_action_mutex.lock();
                // No pending action, block and wait for new action
                if (wait_list_action_queue.empty()) {
                    wait_list_action_mutex.unlock();
                    return false;
                } else {
                    RunID action = wait_list_action_queue.front();
                    wait_list_action_queue.pop();
                    wait_list_action_mutex.unlock();

                    if (action == -1) {
                        DCOUT("Wait list handler received action new run");
                        return true;
                    } else {
                        run_records_mutex.lock();
                        auto run_it = run_records.find(action);
                        ASSERT(run_it != run_records.end());
                        run_it->second->is_done = true;
                        run_records_mutex.unlock();
                        DCOUT("Wait list handler received action run " << action << " completed");
                        return true;
                    }
                }
            });

            run_records_mutex.lock();
            // Check every run in the wait list and find the ones that are ready to be pushed to
            // ready queue
            for (auto wl_it = wait_list.begin(); wl_it != wait_list.end();) {
                RunID run = *wl_it;
                // WARN: need to check if this is correct
                // Here we don't have extra lock for run_records
                for (auto it2 = run_records[run]->deps.begin();
                     it2 != run_records[run]->deps.end();) {
                    auto dep_it = run_records.find(*it2);
                    ASSERT(dep_it != run_records.end());
                    if (dep_it->second->is_done)
                        it2 = run_records[run]->deps.erase(it2);
                    else
                        ++it2;
                }

                // If there are no more dependencies, push the run to the ready queue and remove it
                // from wait list
                if (run_records[run]->deps.empty()) {
                    {
                        std::lock_guard<std::mutex> lock(ready_queue_mutex);
                        ready_queue.push(run);
                    }
                    // Tell ready_queue_handler that there is a new run to process
                    ready_queue_signal.notify_one();
                    wl_it = wait_list.erase(wl_it);
                } else {
                    ++wl_it;
                }
            }
            run_records_mutex.unlock();
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

    DCOUT("Run #" << run_id << " requested");

    // Record the run information
    // No need for lock here: this is the only thread that would create new records and no one
    // can access it until it's created

    {
        std::lock_guard<std::mutex> lock(run_records_mutex);
        RunInfo* run_info = new RunInfo{runnable, num_total_tasks, deps};
        // run_records.emplace(run_id, RunInfo(runnable, num_total_tasks, deps));
        run_records[run_id] = run_info;
    }

    DCOUT("Run #" << run_id << " recorded");

    // NOTE: if we try to check deps here, then it's possible that `wait_list_handler` marks a
    // run done after we checked it to be false, so we will push it to the wait list. This can
    // make things complicated. To avoid, need to either lock the entire run_records or each
    // individual run when checking its deps, which can be bad for performance.
    // So, I choose to always push to wait list if there's any deps at all without checking. The
    // following code is hence commented out

    // // Check the run dependencies, remove the ones that are already done
    // // Note we are using run_info.deps instead of deps, because we want to modify it
    // // Note I assume the deps must be runs that have already been dispatched, so the record
    // // should exist
    // for (auto it = run_records[run_id].deps.begin(); it != run_records[run_id].deps.end();) {
    //     if (run_records[*it].is_done)
    //         it = run_records[run_id].deps.erase(it);
    //     else
    //         ++it;
    // }

    // No need t olock here because only this thread may insert to run_records
    if (run_records[run_id]->deps.empty()) {
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
            // Lock the action mutex to prevent other threads from modifying it before it gets
            // processed
            wait_list.push_back(run_id);
            DCOUT("Run #" << run_id << " pushed to wait list");
        }
        // Tell wait_list_handler that there's new item pushed and also check if it's ready to
        // New item pushed to wait list, mark wait list active
        wait_list_active_signal.notify_one();

        {
            std::lock_guard<std::mutex> lock(wait_list_action_mutex);
            wait_list_action_queue.push(-1);
            DCOUT("Main thread pushed new run #" << run_id << " to action queue");
        }
        // Notify `wait_list_action` is set
        wait_list_action_signal.notify_one();
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
        sync_wait_list = true;
        DCOUT("Wait list sync called");
    }
    wait_list_active_signal.notify_one();

    // Must make sure wait list is emptied before syncing the ready queue
    wait_list_handler_thread.join();

    {
        std::lock_guard<std::mutex> lock(ready_queue_mutex);
        sync_ready_queue = true;
        DCOUT("Ready queue sync called");
    }
    ready_queue_signal.notify_one();

    ready_queue_handler_thread.join();

    // Now we can sync all worker threads
    {
        std::lock_guard<std::mutex> lock(task_queue_mutex);
        sync_workers = true;
        DCOUT("Worker threads sync called");
    }
    worker_signal.notify_all();

    // Join all threads in the thread pool
    for (std::thread& thread : thread_pool) {
        thread.join();
    }

    return;
}

// TODO: looks like I need to implement this for part b as well
void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}
