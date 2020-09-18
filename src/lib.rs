//! An worker takes tasks and runs them.
//! When a task is complete the task calls the task's waker
//! which wakes up any task waiting on the parent.
//!
//! Tasks can be awaited which reschedules the task when children call the waker.
//! Tasks are scheduled on thread local workers.
//!
//! All workers must be created at once.
//! Tasks do not immediately begin work otherwise they would not know where to awake.

// ---------------------

// Currently tasks bounce between threads unnecessarily. 
// Other threads should only be given an opportunity to steal if the current thread 
// has multiple chunks of work it's awaiting.

use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Condvar, Mutex};
use std::task::Waker;
use std::task::{Context, Poll};
thread_local! {
    // The task queue and the other task queues should be separately borrowable
    // They can be stored in a struct with RefCells for each component that needs to be borrowed.
    pub static WORKER: RefCell<Worker<'static>> = RefCell::new(Worker::new());
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
    task_queue: TaskQueue<'static>,
    other_task_queues: Vec<TaskQueue<'static>>,
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

struct SharedState<T> {
    result: Mutex<Option<T>>,
    waker: Mutex<Option<Waker>>,
}

pub struct JoinHandle<'a, T> {
    shared_state: Arc<SharedState<T>>,
    task: Cell<Option<Task<'a>>>,
    phantom: std::marker::PhantomData<T>,
}

impl<T: 'static> JoinHandle<'static, T> {
    pub fn is_complete(&self) -> Poll<T> {
        self.inner_poll()
    }

    /// Runs the task if it's not going to be directly awaited
    pub fn run(&self) {
        if let Some(task) = self.task.take() {
            WORKER.with(|w| w.borrow().enqueue_task(task))
        }
    }

    pub fn inner_poll(&self) -> Poll<T> {
        // If we can't acquire the lock then likely the result is in the process of being stored.
        // Since we don't want to block ever, treat this as a result that's pending.
        // Hopefully contention occurs infrequently.
        if let Ok(mut data) = self.shared_state.result.try_lock() {
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

impl<T: 'static> Future for JoinHandle<'static, T> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        // The context needs to be passed to the call to poll.
        *self.shared_state.waker.lock().unwrap() = Some(context.waker().clone());

        // Now the task can be scheduled.
        // It can only be schedule once.
        if let Some(task) = self.task.take() {
            WORKER.with(|w| w.borrow().enqueue_task(task))
        }

        // Is there a better way to actually poll the task instantly instead of always adding it to the queue.
        self.inner_poll()
    }
}

//struct Task {}

// The use of `Mutex` here may introduce unnecessary indirection.
// But apparently that's just an implementation detail of the standard library
// and `parking-lot`'s `Mutex` may not have the same problem.
type TaskQueue<'a> = Arc<Mutex<VecDeque<Arc<Task<'a>>>>>;

type TaskFnMut<'a> = dyn FnMut(Waker) -> bool + Send + 'a;
// The inner Future is optionally passed in and out.
struct Task<'a> {
    inner_task: Mutex<Option<Box<TaskFnMut<'a>>>>,
}
impl<'a> Task<'a> {
    pub fn new(task: impl FnMut(Waker) -> bool + Send + 'a) -> Self {
        Self {
            inner_task: Mutex::new(Some(Box::new(task))),
        }
    }
    pub fn run(self: &Arc<Task<'a>>, worker: &Worker<'a>) {
        let mut lock = self.inner_task.lock().unwrap();
        if let Some(mut task) = lock.take() {
            let task_queue = worker.task_queue.clone();
            let waker = worker_enqueue_waker::create(self.clone(), task_queue);
            let is_complete = task(waker);

            // If the inner task isn't finished then return the task so it can be executed.
            // There is no race condition here because the waker needs to wait on the lock
            // to get the task to enqueue.
            if !is_complete {
                *lock = Some(task);
            }
        }
    }
}

// Each worker has its own tasks.
// Other workers can steal those tasks.
// A worker can go idle and wait for a task.
// Idling workers will wait on some sort of notification that a new task
// has been added.
pub struct Worker<'a> {
    id: usize,
    new_task: Arc<(Mutex<bool>, Condvar)>,
    task_queue: TaskQueue<'a>,
    other_task_queues: Vec<TaskQueue<'a>>,
}

impl<'a> Worker<'a> {
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

    fn enqueue_task(&self, task: Task<'a>) {
        println!("ENQUEING TASK");
        self.task_queue.lock().unwrap().push_back(Arc::new(task));

        // Notify other threads that work is available to steal.
        let (lock, condvar) = &*self.new_task;
        let mut new_task = lock.lock().unwrap();
        *new_task = true;

        // Only wake one waiting worker to avoid contention.
        condvar.notify_one();
    }

    pub fn spawn<T: Send + 'a>(
        &self,
        // mut task: impl FnMut() -> T + Send + 'static,
        future: impl Future<Output = T> + Send + 'a,
    ) -> JoinHandle<'a, T> {
        let shared_state = Arc::new(SharedState {
            result: Mutex::new(None),
            waker: Mutex::new(None),
        });

        let task_shared_state = shared_state.clone();
        // How to create a task to be awoken when the child task completes?
        // This task needs to poll the child, but also enqueue itself to try again.
        // This doesn't strictly need to be a closure. It's just the inner task that needs to be passed around?
        // However it does need to be a closure because the type being stored isn't known without
        // the distinct closure.

        let mut future = Box::pin(future);
        let task = Task::new(Box::new(move |waker: Waker| {
            let context = &mut Context::from_waker(&waker);

            // Run task to completion
            // Need to construct a waker here that references this.
            let data = future.as_mut().poll(context);
            match data {
                Poll::Ready(data) => {
                    *task_shared_state.result.lock().unwrap() = Some(data);
                    task_shared_state
                        .waker
                        .lock()
                        .unwrap()
                        .take()
                        .map(|w| w.wake());
                    true
                }
                Poll::Pending => false,
            }
        }));

        /*
        self.task_queue.lock().unwrap().push_back(Arc::new(task));

        // Notify other threads that work is available to steal.
        let (lock, condvar) = &*self.new_task;
        let mut new_task = lock.lock().unwrap();
        *new_task = true;

        // Only wake one waiting worker to avoid contention.
        condvar.notify_one();
        */

        JoinHandle {
            shared_state,
            task: Cell::new(Some(task)),
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
                if let Some(task) = task {
                    task.run(&self);
                    ran_a_task = true;
                } else {
                    break;
                }
            }

            println!("WORKER {:?}: No tasks in my queue", self.id);

            // If I'm out of work then try to take tasks from other worker's queues
            for q in self.other_task_queues.iter() {
                let task = q.lock().unwrap().pop_front();
                if let Some(task) = task {
                    println!("WORKER {:?}: Stealing task!", self.id);
                    task.run(&self);
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
        if let Some(task) = task {
            task.run(&self);
        }
    }

    pub fn run_current_thread_tasks(&self) {
        // This is separated into two lines so that self.tasks isn't borrowed while running the task
        loop {
            let task = self.task_queue.lock().unwrap().pop_front();
            if let Some(task) = task {
                task.run(&self);
            } else {
                break;
            }
        }
    }

    /// Block until new work arrives.
    pub fn wait_for_work(&mut self) {}
}

pub fn spawn<T: Send + 'static>(
    task: impl Future<Output = T> + Send + 'static,
) -> JoinHandle<'static, T> {
    WORKER.with(|w| w.borrow().spawn(task))
}

pub fn run_current_thread_tasks() {
    WORKER.with(|w| w.borrow().run_current_thread_tasks())
}

/*
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

*/

// This needs to store the TaskQueue to push to and the parent task to wake.
// In what scenarios would a waker be cloned?
// Does the parent task need to be stored in an option that's taken when waking the parent?
mod worker_enqueue_waker {
    use super::{Task, TaskQueue};
    use std::sync::Arc;
    use std::task::{RawWaker, RawWakerVTable, Waker};

    struct WakerData<'a> {
        task: Arc<Task<'a>>,
        task_queue: TaskQueue<'a>,
    }

    pub(crate) fn create<'a>(task: Arc<Task<'a>>, task_queue: TaskQueue<'a>) -> Waker {
        let waker_data = Arc::new(WakerData { task, task_queue });
        let raw_waker = RawWaker::new(Arc::into_raw(waker_data) as *const (), &VTABLE);
        unsafe { Waker::from_raw(raw_waker) }
    }

    const VTABLE: RawWakerVTable = RawWakerVTable::new(clone, wake, wake_by_ref, drop);

    unsafe fn clone(worker_data: *const ()) -> RawWaker {
        let worker_data = Arc::from_raw(worker_data as *const WakerData);
        let cloned_worker_data = worker_data.clone();
        Arc::into_raw(worker_data); // Don't drop the original Arc here.
        RawWaker::new(Arc::into_raw(cloned_worker_data) as *const (), &VTABLE)
    }

    // This consumes the data pointer
    unsafe fn wake(worker_data: *const ()) {
        println!("WAKING");
        let worker_data = Arc::from_raw(worker_data as *const WakerData);
        let task_active = worker_data.task.inner_task.lock().unwrap().is_some();

        if task_active {
            println!("TASK ACTIVE");
            worker_data
                .task_queue
                .lock()
                .unwrap()
                .push_back(worker_data.task.clone()); // Is this clone unnecessary?
        }
        // worker_data is dropped here.
    }

    // Do not consume the data pointer
    unsafe fn wake_by_ref(worker_data: *const ()) {
        println!("WAKING BY REF");

        let worker_data = Arc::from_raw(worker_data as *const WakerData);
        let task_active = worker_data.task.inner_task.lock().unwrap().is_some();

        if task_active {
            worker_data
                .task_queue
                .lock()
                .unwrap()
                .push_back(worker_data.task.clone());
        }
        Arc::into_raw(worker_data);
    }

    unsafe fn drop(worker_data: *const ()) {
        // Convert back to an Arc so that it can be dropped
        let _ = Arc::from_raw(worker_data as *const WakerData);
    }
}
