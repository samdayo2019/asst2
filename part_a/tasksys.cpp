#include "tasksys.h"

IRunnable::~IRunnable() {}

ITaskSystem::ITaskSystem(int num_threads) {}
ITaskSystem::~ITaskSystem() {}

/*
 * ================================================================
 * Serial task system implementation
 * ================================================================
 */

const char* TaskSystemSerial::name() {
    return "Serial";
}

TaskSystemSerial::TaskSystemSerial(int num_threads): ITaskSystem(num_threads) {
}

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

const char* TaskSystemParallelSpawn::name() {
    return "Parallel + Always Spawn";
}

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads): ITaskSystem(num_threads), num_threads(num_threads){
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
    
    /*we need to split the total work amongst the number of threads. 
        The number of threads should match the num_threads.
    */

   /*
    The code below yielded insufficient improvements.
        Strategies for speed up: using non sequential static assignment
   */
    // int thread_tasks = (int) ceil(num_total_tasks / num_threads); 

    // std::vector <std::thread> workers; //instantiate num_threads number of threads.

    // for (int i = 0; i < num_threads; i++){
    //     int starting_task = i*thread_tasks; // offset start for each thread

    //     // deals with case where the number of total task is not perfectly divisible by the number of threads.
    //     int end_task = ((starting_task + thread_tasks) < num_total_tasks)? starting_task + thread_tasks : num_total_tasks;

    //     workers.emplace_back([runnable, starting_task, end_task](){
    //         for (int i = starting_task; i < end_task; ++i) {
    //             runnable->runTask(i, num_total_tasks);
    //         }
    //     });
    // }

    // for ( int i = 0; i < num_threads; ++i){
    //     workers[i].join();
    // }

    // int thread_tasks = (int) ceil(num_total_tasks / num_threads); 

    std::vector <std::thread> workers; //instantiate num_threads number of threads.
    std::atomic<int> task_counter(0); 

    auto runThread = [&task_counter, runnable, num_total_tasks, this](){
        while(true){
            // natch dynamic tasking (batch of 4)
            int task = task_counter.fetch_add(1);
            if (task >= num_total_tasks) break;
            runnable->runTask(task, num_total_tasks);
        }
    };
    
    for(int i =0; i < num_threads; i++){
        workers.emplace_back(runThread);
    }

    for ( int i = 0; i < num_threads; ++i){
        workers[i].join();
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

const char* TaskSystemParallelThreadPoolSpinning::name() {
    return "Parallel + Thread Pool + Spin";
}

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads): ITaskSystem(num_threads), num_threads(num_threads), 
        num_task_completed(0), all_tasks_completed(false) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    // create the threadpool that will be used:
    threadpool.resize(num_threads);
    // printf("num_threads %d", (int)threadpool.size());
    for(int i =0; i < num_threads; i++){
        threadpool[i] = std::thread(&TaskSystemParallelThreadPoolSpinning::runThreadwPool, this); // this performs the thread waiting, access to queues, task calls.
    }

}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {
    all_tasks_completed = true; 

    for(int i = 0; i < num_threads; i++){
        if(threadpool[i].joinable()){
            threadpool[i].join();
        }
    }

}

void TaskSystemParallelThreadPoolSpinning::runThreadwPool(){
    while(!all_tasks_completed){
        int executing_task = -1;  // for checking whether we have a valid task to run

        {
            std::lock_guard<std::mutex> lock(protect_mutex); // lock the protection mutex.
            // check if the queue is empty, if it it not take a task from the front of the queue and remove it
            if(!task_queue.empty()){
                executing_task = task_queue.front();
                task_queue.pop();
            }
            
        }

        if(executing_task != -1){
            runnable_obj->runTask(executing_task, num_tasks);
            num_task_completed.fetch_add(1); // add 1 to the running total of completed tasks
        }
        else{
            std::this_thread::yield(); // let other threads have control if nothing to do (the queue is empty --> give control to main thread)
        }
    }
}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {
    runnable_obj = runnable;
    num_tasks = num_total_tasks;
    //
    // TODO: CS149 students will modify the implementation of this
    // method in Part A.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

    {
        std::lock_guard<std::mutex> lock(protect_mutex); // lock the protection mutex.
            // check if the queue is empty, if it it not take a task from the front of the queue and remove it
    
        for(int i = 0; i < num_tasks; i++){
            task_queue.push(i);
        }
    }

    
    while(num_task_completed.load() < num_total_tasks) std::this_thread::yield();

    num_task_completed.store(0);
    
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
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
    : ITaskSystem(num_threads), num_threads(num_threads), num_task_completed(0), all_tasks_completed(false) {
    threadpool.resize(num_threads);
    for (int i = 0; i < num_threads; i++) {
        threadpool[i] = std::thread(&TaskSystemParallelThreadPoolSleeping::runThreadwPool, this);
    }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    {
        std::lock_guard<std::mutex> lock(protect_mutex);
        all_tasks_completed = true;
    }
    cv_task.notify_all();

    for (int i = 0; i < num_threads; i++) {
        if (threadpool[i].joinable()) {
            threadpool[i].join();
        }
    }
}

void TaskSystemParallelThreadPoolSleeping::runThreadwPool() {
    while (true) {
        int executing_task = -1;
        {
            std::unique_lock<std::mutex> lock(protect_mutex);
            cv_task.wait(lock, [this] {
                return !task_queue.empty() || all_tasks_completed;
            });

            if (all_tasks_completed && task_queue.empty()) {
                break;
            }

            if (!task_queue.empty()) {
                executing_task = task_queue.front();
                task_queue.pop();
            }
        }
        
        if (executing_task != -1) {
            runnable_obj->runTask(executing_task, num_tasks);
            int completed = num_task_completed.fetch_add(1) + 1;

            if (completed == num_tasks) {
                std::lock_guard<std::mutex> lock(protect_mutex);
                cv_completed.notify_one();
            }
        }
    }
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {
    runnable_obj = runnable;
    num_tasks = num_total_tasks;

    {
        std::lock_guard<std::mutex> lock(protect_mutex);
        for (int i = 0; i < num_tasks; i++) {
            task_queue.push(i);
        }
    }

    cv_task.notify_all();

    std::unique_lock<std::mutex> lock(protect_mutex);
    cv_completed.wait(lock, [this] {
        return num_task_completed.load() == num_tasks;
    });

    num_task_completed.store(0);
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelThreadPoolSleeping::sync() {
    // You do not need to implement this method.
    return;
}
