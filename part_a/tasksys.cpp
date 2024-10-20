#include "tasksys.h"
#include <cassert>
#include <thread>

#ifdef DEBUG
#include <iostream>
#include <sstream>
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
#else
#define DPRINTF(...)
#define DCOUT(...)
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
    // You do not need to implement this method.
    return 0;
}

void TaskSystemSerial::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelSpawn::name() { return "Parallel + Always Spawn"; }

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads) : ITaskSystem(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {
    //
    // TODO: CS149 students will modify the implementation of this
    // method in Part A.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelSpawn::sync() {
    // You do not need to implement this method.
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
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {
    //
    // TODO: CS149 students will modify the implementation of this
    // method in Part A.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable,
                                                              int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // You do not need to implement this method.
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

    // Pre-allocate the thread pool so that it won't resize as we push
    thread_pool.reserve(num_threads);

    this->num_threads = num_threads;

    // Create `num_threads` threads in the thread pool
    // Each thread will run the `thread_worker` function
    for (int i = 0; i < num_threads; i++) {
        thread_pool.emplace_back(&TaskSystemParallelThreadPoolSleeping::thread_worker, this);
    }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {

    // Notify the worker threads to stop
    {
        std::lock_guard<std::mutex> lock(task_queue_mutex);
        stop = true;
    }
    run_signal.notify_all();

    // Join all threads in the thread pool
    for (std::thread& thread : thread_pool) {
        thread.join();
    }

    // #ifdef DEBUG
    //     // Print out the number of tasks completed by each thread
    //     for (auto& pair : tasks_per_thread) {
    //         DCOUT("Thread " << pair.first << " completed " << pair.second << " tasks");
    //     }
    // #endif

    DCOUT("TaskSystemParallelThreadPoolSleeping destroyed")
}

void TaskSystemParallelThreadPoolSleeping::thread_worker(void) {
#ifdef DEBUG
    tasks_per_thread[std::this_thread::get_id()] = 0;
#endif
    // Worker thread runs forever
    while (true) {
        int task;
        // Create a scope for lock lifetime
        {
            // Lock the task queue when accessing it
            std::unique_lock<std::mutex> lock(task_queue_mutex);
            // Wait for (1) a task to be enqueued or (2) the stop signal to be sent
            // Calling wait automatically unlocks the mutex and then locks it again
            // The predicate ensures wait is only entered if the task queue is empty and not
            // stopped. This handles the initial missed notification issue
            run_signal.wait(lock, [this] { return !task_queue.empty() || stop; });

            // If the signal was stop, check if we've finished all tasks, if not then work on it,
            // otherwise we are done
            // Note if there are more tasks remaining, then the loop is going to start over again,
            // but the wait won't be entered because stop should be true
            if (stop && task_queue.empty()) {
                DCOUT("Thread " << std::this_thread::get_id() << " exiting");
                return;
            }
            assert(!task_queue.empty());
            // Otherwise, there must be a task in the queue
            task = task_queue.front();
            task_queue.pop_front();

            // Upon exiting the scope, lock is automatically released, so other threads can access
            // the queue
        }

        // Run the task
        runnable->runTask(task, num_total_tasks);
#ifdef DEBUG
        tasks_per_thread[std::this_thread::get_id()]++;
#endif

        // Increment the completed tasks atomically
        {
            std::lock_guard<std::mutex> lock(num_tasks_completed_mutex);
            num_tasks_completed.fetch_add(1);
        }
        task_completed_signal.notify_one();
    }
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {

    // Since the next run starts after the previous run is completely done, we can safely overwrite
    // this shared variable
    this->num_total_tasks = num_total_tasks;
    // Since a single runnable functin is shared by all runTask calls, just need to store it once
    this->runnable = runnable;

    // Pushing data at the front of the queue
    int batch = 4;
    int i = 0;
    while (i < num_total_tasks) {
        int bound = std::min(batch, num_total_tasks - i);
        {
            std::unique_lock<std::mutex> lock(task_queue_mutex);
            for (int j = 0; j < bound; j++) {
                task_queue.push_back(i++);
            }
        }
        run_signal.notify_all();
    }

    // for (int i = 0; i < num_total_tasks; i++) {
    //     {
    //         std::unique_lock<std::mutex> lock(task_queue_mutex);
    //         task_queue.push_back(i);
    //     }
    //     run_signal.notify_one();
    // }

    // {
    //     std::lock_guard<std::mutex> lock(task_queue_mutex);
    //     for (int i = 0; i < num_total_tasks; i++) {
    //         task_queue.push_back(i);
    //     }
    // }
    // run_signal.notify_all();

    // Lock when reading num_tasks_completed
    {
        std::unique_lock<std::mutex> lock(num_tasks_completed_mutex);
        // Spin until all tasks are completed
        task_completed_signal.wait(lock, [this, num_total_tasks] {
            return num_tasks_completed.load() == num_total_tasks;
        });
    }
    // Reset the number of tasks completed
    num_tasks_completed.store(0);

#ifdef DEBUG
    // Print out the number of tasks completed by each thread
    // Somehow this prints some threads twice..
    for (auto& pair : tasks_per_thread) {
        DCOUT("Thread " << pair.first << " completed " << pair.second << " tasks");
        pair.second = 0;
    }
#endif
    DPRINTF("All tasks completed\n");
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable,
                                                              int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    //
    // TODO: CS149 students will implement this method in Part B.
    //

    return 0;
}

void TaskSystemParallelThreadPoolSleeping::sync() {
    //
    // TODO: CS149 students will modify the implementation of this method in
    // Part B.
    //

    return;
}
