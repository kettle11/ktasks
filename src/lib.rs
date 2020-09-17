//! An worker takes tasks and runs them.
//! When a task is complete the task calls the task's waker
//! which wakes up any task waiting on the parent.
//!
//! Tasks can be awaited which reschedules the task when children call the waker.
//! Tasks are scheduled on thread local workers.
//!
//! All workers must be created at once.

//! To-do:
//! Make async waker work properly
//! Make tasks not run until they're polled.
//! Make way for tasks to not leave main thread.
//!
//! The entire thread local worker shouldn't be borrowed to run the worker
//! Nested spawns crash if this is done

use std::cell::RefCell;
use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Condvar, Mutex};
use std::task::{Context, Poll};

thread_local! {
    // The task queue and the other task queues should be separately borrowable
    // They can be stored in a struct with RefCells for each component that needs to be borrowed.
    pub static WORKER: RefCell<Worker> = RefCell::new(Worker::new());
}
/// Create workers for other threads and returns this thread's worker.

pub fn create_workers(count: u32) {
    assert!(count > 0);

    // Used to signal that a new task has been added.
    // Wakes other threads that may be waiting to potentially steal work.
    let new_task = Arc::new((Mutex::new(false), Condvar::new()));

    // Create the task queues for the workers
    let queues: Vec<TaskQueue> = (0..count)
        .map(|_| Arc::new(Mutex::new(VecDeque::new())))
        .collect();

    // Create a worker for this thread.
    let mut other_task_queues = queues.clone();
    other_task_queues.swap_remove(0);
    create_worker(0, queues[0].clone(), other_task_queues, new_task.clone());

    // Create workers for other threads.
    for (i, task_queue) in queues.iter().cloned().enumerate().skip(1) {
        let mut other_task_queues = queues.clone();
        other_task_queues.swap_remove(i);
        let new_task = new_task.clone();
        std::thread::spawn(move || {
            println!("ID: {:?}", i);
            create_worker(i, task_queue, other_task_queues, new_task);

            // Run the other threads forever waiting for work.
            WORKER.with(|w| {
                w.borrow().run_forever();
            });
        });
    }
}

fn create_worker(
    id: usize,
    task_queue: TaskQueue,
    other_task_queues: Vec<TaskQueue>,
    new_task: Arc<(Mutex<bool>, Condvar)>,
) {
    WORKER.with(|w| {
        let mut w = w.borrow_mut();
        w.id = id;
        w.task_queue = task_queue;
        w.other_task_queues = other_task_queues;
        w.new_task = new_task;
    });
}

pub struct JoinHandle<T> {
    result: Arc<Mutex<Option<T>>>,
    phantom: std::marker::PhantomData<T>,
}

impl<T: 'static> JoinHandle<T> {
    pub fn is_complete(&self) -> Poll<T> {
        self.inner_poll()
    }

    pub fn inner_poll(&self) -> Poll<T> {
        // The task should be kicked off here if it's not already.

        // If we can't acquire the lock then likely the result is in the process of being stored.
        // Since we don't want to block ever, treat this as a result that's pending.
        // Hopefully contention occurs infrequently.
        if let Ok(mut data) = self.result.try_lock() {
            if let Some(data) = data.take() {
                Poll::Ready(data)
            } else {
                Poll::Pending
            }
        } else {
            Poll::Pending
        }
    }
}

impl<T: 'static> Future for JoinHandle<T> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        // Here the future should be polled for the first time, which will enqueue it.
        // Rust futures shouldn't run until they're polled?
        self.inner_poll()
    }
}

struct Task {}

type TaskQueue = Arc<Mutex<VecDeque<Box<dyn FnMut() + Send>>>>;

// Each worker has its own tasks.
// Other workers can steal those tasks.
// A worker can go idle and wait for a task.
// Idling workers will wait on some sort of notification that a new task
// has been added.
pub struct Worker {
    id: usize,
    new_task: Arc<(Mutex<bool>, Condvar)>,
    task_queue: TaskQueue,
    other_task_queues: Vec<TaskQueue>,
}

impl Worker {
    pub fn new() -> Self {
        // These members are placeholders and will be replaced later.
        // Which isn't the best approach, but it works for now.
        Self {
            id: 0,
            new_task: Arc::new((Mutex::new(false), Condvar::new())),
            task_queue: Arc::new(Mutex::new(VecDeque::new())),
            other_task_queues: Vec::new(),
        }
    }

    pub fn spawn<T: Send + 'static>(
        &self,
        mut task: impl FnMut() -> T + Send + 'static,
        // future: impl Future<Output = T> + Send + 'static,
    ) -> JoinHandle<T> {
        let result = Arc::new(Mutex::new(None));
        let thread_result = result.clone();

        self.task_queue.lock().unwrap().push_back(Box::new(move || {
            // Run task to completion
            let data = task();
            // Store the result in the JoinHandle
            *thread_result.lock().unwrap() = Some(data);
        }));

        // Notify other threads that work is available to steal.
        let (lock, condvar) = &*self.new_task;
        let mut new_task = lock.lock().unwrap();
        *new_task = true;

        // Only wake one waiting worker to avoid contention.
        condvar.notify_one();

        JoinHandle {
            result,
            phantom: std::marker::PhantomData,
        }
    }

    /// Run forever and block when waiting for new work.
    pub fn run_forever(&self) {
        let mut ran_a_task;
        loop {
            ran_a_task = false;

            // Keep running tasks from my own queue
            loop {
                let task = self.task_queue.lock().unwrap().pop_front();
                if let Some(mut task) = task {
                    task();
                    ran_a_task = true;
                } else {
                    break;
                }
            }

            println!("WORKER {:?}: No tasks in my queue", self.id);

            // If I'm out of work then try to take tasks from other queues
            for q in self.other_task_queues.iter() {
                let task = q.lock().unwrap().pop_front();
                if let Some(mut task) = task {
                    println!("Stealing task!");
                    task();
                    ran_a_task = true;
                    // Only steal a single task before checking my own queue
                    break;
                }
            }

            // If no tasks were available then go to sleep until tasks are available
            if !ran_a_task {
                println!("WORKER {:?}: Waiting for tasks to steal", self.id);

                // If I reach here then block until other tasks are enqueued.
                let (lock, condvar) = &*self.new_task;
                let mut new_task = lock.lock().unwrap();
                while !*new_task {
                    new_task = condvar.wait(new_task).unwrap();
                }
                *new_task = false;

                println!("WORKER {:?}: Woken up to steal a task", self.id);
            }
        }
    }

    pub fn run_task(&mut self) {
        // This is separated into two lines so that self.tasks isn't borrowed while running the task
        let task = self.task_queue.lock().unwrap().pop_front();
        if let Some(mut task) = task {
            task();
        }
    }

    pub fn run_tasks(&self) {
        // This is separated into two lines so that self.tasks isn't borrowed while running the task
        loop {
            let task = self.task_queue.lock().unwrap().pop_front();
            if let Some(mut task) = task {
                task();
            } else {
                break;
            }
        }
    }

    /// Block until new work arrives.
    pub fn wait_for_work(&mut self) {}
}

pub fn spawn<T: Send + 'static>(task: impl FnMut() -> T + Send + 'static) -> JoinHandle<T> {
    WORKER.with(|w| w.borrow().spawn(task))
}

pub fn run_tasks() {
    // This shouldn't borrow the worker because then new tasks cannot be spawned.
    WORKER.with(|w| w.borrow().run_tasks())
}

mod do_nothing_waker {
    use std::task::{RawWaker, RawWakerVTable, Waker};

    pub fn create() -> Waker {
        unsafe { Waker::from_raw(RAW_WAKER) }
    }

    const RAW_WAKER: RawWaker = RawWaker::new(std::ptr::null(), &VTABLE);
    const VTABLE: RawWakerVTable = RawWakerVTable::new(clone, wake, wake_by_ref, drop);

    unsafe fn clone(_: *const ()) -> RawWaker {
        RAW_WAKER
    }
    unsafe fn wake(_: *const ()) {}
    unsafe fn wake_by_ref(_: *const ()) {}
    unsafe fn drop(_: *const ()) {}
}

mod worker_enqueue_waker {
    use std::task::{RawWaker, RawWakerVTable, Waker};

    pub fn create() -> Waker {
        unsafe { Waker::from_raw(RAW_WAKER) }
    }

    const RAW_WAKER: RawWaker = RawWaker::new(std::ptr::null(), &VTABLE);
    const VTABLE: RawWakerVTable = RawWakerVTable::new(clone, wake, wake_by_ref, drop);

    unsafe fn clone(_: *const ()) -> RawWaker {
        RAW_WAKER
    }
    unsafe fn wake(_: *const ()) {}
    unsafe fn wake_by_ref(_: *const ()) {}
    unsafe fn drop(_: *const ()) {}
}
