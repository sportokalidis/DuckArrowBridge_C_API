#include "thread_pool.hpp"

#include <iostream>

// Constructor
ThreadPool::ThreadPool(size_t max_threads) : stop(false) {
    for (size_t i = 0; i < max_threads; ++i) {
        //std::cout << "Creating thread " << i << std::endl;
        workers.emplace_back([this] {
            while (true) {
                std::function<void()> task;
                {
                    std::unique_lock<std::mutex> lock(this->queue_mutex);
                    this->condition.wait(lock, [this] { return this->stop || !this->tasks.empty(); });
                    if (this->stop && this->tasks.empty())
                        return;
                    task = std::move(this->tasks.front());
                    this->tasks.pop();
                }
                //std::cout << "Thread " << std::this_thread::get_id() << " is executing a task." << std::endl;
                task();
            }
        });
    }
}

ThreadPool::~ThreadPool() {
    {
        std::unique_lock<std::mutex> lock(queue_mutex);
        stop = true;
    }
    condition.notify_all();
    for (std::thread &worker : workers)
        worker.join();
}