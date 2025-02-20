//! A worker takes tasks and runs them.
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

// It may be worth replacing some very short mutex locks with spinlocks.
// This would allow the code to work properly on the main thread on Wasm.

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
    static WORKER: RefCell<Worker<'static>> = RefCell::new(Worker::new());
}

/// Create workers for this thread and other threads.
/// Count must be 1 or greater.
/// This should only be called once.
pub fn create_workers(count: u32) {
    use std::sync::Once;
    static SETUP: Once = Once::new();

    SETUP.call_once(|| {
        assert!(count > 0);

        // Used to signal that a new task has been added.
        // Wakes other threads that may be waiting to potentially steal work.
        let worker_waker = WorkerWaker::new();

        // Create the task queues for the workers
        let queues: Vec<TaskQueue> = (0..count)
            .map(|_| Arc::new(Mutex::new(VecDeque::new())))
            .collect();

        // Create a worker for this thread.
        let mut other_task_queues = queues.clone();
        other_task_queues.swap_remove(0);

        let main_thread_local_task_queue = WORKER.with(|w| w.borrow().local_task_queue.clone());
        create_worker(
            0,
            queues[0].clone(),
            main_thread_local_task_queue.clone(),
            other_task_queues,
            worker_waker.clone(),
        );

        // Create workers for other threads.
        // Only create workers for non-WebAssembly platforms, or WebAssembly if atomics are enabled.
        #[cfg(any(not(target_arch = "wasm32"), target_feature = "atomics"))]
        for (i, task_queue) in queues.iter().cloned().enumerate().skip(1) {
            let main_thread_local_task_queue = main_thread_local_task_queue.clone();
            let mut other_task_queues = queues.clone();
            other_task_queues.swap_remove(i);
            let worker_waker = worker_waker.clone();
            let closure = move || {
                // println!("ID: {:?}", i);
                create_worker(
                    i,
                    task_queue,
                    main_thread_local_task_queue,
                    other_task_queues,
                    worker_waker,
                );

                // Run the other threads forever waiting for work.
                WORKER.with(|w| {
                    w.borrow().run_forever();
                });
            };

            #[cfg(not(target_arch = "wasm32"))]
            std::thread::spawn(closure);

            #[cfg(all(target_arch = "wasm32", target_feature = "atomics"))]
            kwasm::web_worker::spawn(closure);
        }
    });
}

#[derive(Clone)]
// Blocks a single thread.
struct WorkerWaker(Arc<(Mutex<bool>, Condvar)>);

impl WorkerWaker {
    fn new() -> Self {
        Self(Arc::new((Mutex::new(false), Condvar::new())))
    }

    fn wake_one(&self) {
        // Notify other threads that work is available to steal.
        let (lock, condvar) = &*self.0;
        let mut new_task = lock.lock().unwrap();
        *new_task = true;
        condvar.notify_one();
    }

    fn wake_all(&self) {
        // Notify other threads that work is available to steal.
        let (lock, condvar) = &*self.0;
        let mut new_task = lock.lock().unwrap();
        *new_task = true;
        condvar.notify_all();
    }

    fn wait(&self) {
        // If I reach here then block until other tasks are enqueued.
        let (lock, condvar) = &*self.0;
        let mut new_task = lock.lock().unwrap();
        while !*new_task {
            new_task = condvar.wait(new_task).unwrap();
        }
        *new_task = false;
    }
}

fn create_worker(
    id: usize,
    task_queue: TaskQueue<'static>,
    main_thread_local_task_queue: LocalTaskQueue<'static>,
    other_task_queues: Vec<TaskQueue<'static>>,
    new_task: WorkerWaker,
) {
    WORKER.with(|w| {
        let mut w = w.borrow_mut();
        w.id = id;
        w.task_queue = task_queue;
        w.main_thread_local_task_queue = main_thread_local_task_queue;
        w.other_task_queues = other_task_queues;
        w.new_task = new_task;
    });
}

struct SharedState<T> {
    result: Mutex<Option<T>>,
    waker: Mutex<Option<Waker>>,
}

enum JoinHandleInnerTask<'a> {
    Local(LocalTask<'a>, LocalTaskQueue<'static>),
    NonLocal(Task<'a>),
}
pub struct JoinHandle<'a, T> {
    shared_state: Arc<SharedState<T>>,
    task: Cell<Option<JoinHandleInnerTask<'a>>>,
    phantom: std::marker::PhantomData<T>,
}

// Safety: JoinHandle contains an inner function that will only be invoked on the thread that originated it.
// But other threads can enqueue that task to wake it elsewhere.
unsafe impl<'a, T> Send for JoinHandle<'a, T> {}

impl<T: 'static> JoinHandle<'static, T> {
    pub fn is_complete(&self) -> Poll<T> {
        self.inner_poll()
    }

    /// Runs the task if it's not going to be directly awaited
    pub fn run(&self) {
        if let Some(task) = self.task.take() {
            match task {
                JoinHandleInnerTask::Local(task, local_task_queue) => {
                    local_task_queue.0.lock().unwrap().push_back(Arc::new(task));
                }
                JoinHandleInnerTask::NonLocal(task) => {
                    WORKER.with(|w| w.borrow().enqueue_task(task))
                }
            }
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
        // It can only be scheduled once.
        if let Some(task) = self.task.take() {
            match task {
                JoinHandleInnerTask::Local(task, local_task_queue) => {
                    local_task_queue.0.lock().unwrap().push_back(Arc::new(task));
                }
                JoinHandleInnerTask::NonLocal(task) => {
                    WORKER.with(|w| w.borrow().enqueue_task(task))
                }
            }
        }

        // Is there a better way to actually poll the task instantly instead of always adding it to the queue.
        self.inner_poll()
    }
}

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
            let waker =
                worker_enqueue_waker::create(self.clone(), task_queue, worker.new_task.clone());
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

#[derive(Clone)]
struct LocalTaskQueue<'a>(Arc<Mutex<VecDeque<Arc<LocalTask<'a>>>>>);

// Safety: The inner members of LocalTaskQueue are never directly accessed on other threads.
unsafe impl<'a> Send for LocalTaskQueue<'a> {}

struct LocalTask<'a> {
    inner_task: RefCell<Option<Box<dyn FnMut(Waker) -> bool + 'a>>>,
}

impl<'a> LocalTask<'a> {
    pub fn new(task: impl FnMut(Waker) -> bool + 'a) -> Self {
        Self {
            inner_task: RefCell::new(Some(Box::new(task))),
        }
    }

    pub fn run(self: &Arc<LocalTask<'a>>, worker: &Worker<'a>) {
        let task = self.inner_task.borrow_mut().take();
        if let Some(mut task) = task {
            let task_queue = worker.local_task_queue.clone();
            let waker = worker_enqueue_waker_local::create(
                self.clone(),
                task_queue,
                worker.new_task.clone(),
            );
            let is_complete = task(waker);

            // If the inner task isn't finished then return the task so it can be executed.
            // There is no race condition here because the waker needs to wait on the lock
            // to get the task to enqueue.
            if !is_complete {
                *self.inner_task.borrow_mut() = Some(task);
            }
        }
    }
}

// Each worker has its own tasks.
// Other workers can steal those tasks.
// A worker can go idle and wait for a task.
// Idling workers will wait on some sort of notification that a new task
// has been added.
struct Worker<'a> {
    #[allow(unused)]
    id: usize,
    new_task: WorkerWaker,
    task_queue: TaskQueue<'a>,
    local_task_queue: LocalTaskQueue<'a>,
    main_thread_local_task_queue: LocalTaskQueue<'a>,
    other_task_queues: Vec<TaskQueue<'a>>,
}

// Safety: Workers are not automatically Send because they contain a LocalTaskQueue,

impl<'a> Worker<'a> {
    pub fn new() -> Self {
        // These members are placeholders and will be replaced later.
        // Which isn't the best approach, but it works for now.
        Self {
            id: 0,
            new_task: WorkerWaker::new(),
            local_task_queue: LocalTaskQueue(Arc::new(Mutex::new(VecDeque::new()))),
            task_queue: Arc::new(Mutex::new(VecDeque::new())),
            main_thread_local_task_queue: LocalTaskQueue(Arc::new(Mutex::new(VecDeque::new()))),
            other_task_queues: Vec::new(),
        }
    }

    fn enqueue_task(&self, task: Task<'a>) {
        // println!("ENQUEING TASK");
        self.task_queue.lock().unwrap().push_back(Arc::new(task));
        self.new_task.wake_one()
    }

    pub fn spawn<T: Send + 'a>(
        &self,
        future: impl Future<Output = T> + Send + 'a,
    ) -> JoinHandle<'a, T> {
        let shared_state = Arc::new(SharedState {
            result: Mutex::new(None),
            waker: Mutex::new(None),
        });

        let task_shared_state = shared_state.clone();

        let mut future = Box::pin(future);
        let task = Task::new(Box::new(move |waker: Waker| {
            let context = &mut Context::from_waker(&waker);

            // Run task to completion
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

        JoinHandle {
            shared_state,
            task: Cell::new(Some(JoinHandleInnerTask::NonLocal(task))),
            phantom: std::marker::PhantomData,
        }
    }

    pub fn spawn_main<T: Send + 'a>(
        &self,
        future: impl Future<Output = T> + Send + 'a,
    ) -> JoinHandle<'a, T> {
        let shared_state = Arc::new(SharedState {
            result: Mutex::new(None),
            waker: Mutex::new(None),
        });

        let task_shared_state = shared_state.clone();

        let mut future = Box::pin(future);
        let task = LocalTask::new(Box::new(move |waker: Waker| {
            let context = &mut Context::from_waker(&waker);

            // Run task to completion
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

        let main_thread_queue = WORKER.with(|w| w.borrow().main_thread_local_task_queue.clone());
        JoinHandle {
            shared_state,
            task: Cell::new(Some(JoinHandleInnerTask::Local(task, main_thread_queue))),
            phantom: std::marker::PhantomData,
        }
    }

    pub fn spawn_local<T: 'a>(&self, future: impl Future<Output = T> + 'a) -> JoinHandle<'a, T> {
        let shared_state = Arc::new(SharedState {
            result: Mutex::new(None),
            waker: Mutex::new(None),
        });

        let task_shared_state = shared_state.clone();

        let mut future = Box::pin(future);
        let task = LocalTask::new(Box::new(move |waker: Waker| {
            let context = &mut Context::from_waker(&waker);

            // Run task to completion
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

        let local_task_queue = WORKER.with(|worker| worker.borrow().local_task_queue.clone());
        JoinHandle {
            shared_state,
            task: Cell::new(Some(JoinHandleInnerTask::Local(task, local_task_queue))),
            phantom: std::marker::PhantomData,
        }
    }

    /// Run forever and block when waiting for new work.
    #[allow(unused)]
    pub fn run_forever(&self) {
        let mut ran_a_task;
        loop {
            ran_a_task = false;

            // First run tasks from my local queue.
            loop {
                let task = self.local_task_queue.0.lock().unwrap().pop_front();
                if let Some(task) = task {
                    task.run(&self);
                    ran_a_task = true;
                } else {
                    break;
                }
            }

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

            // println!("WORKER {:?}: No tasks in my queue", self.id);

            // If I'm out of work then try to take tasks from other worker's queues
            for q in self.other_task_queues.iter() {
                let task = q.lock().unwrap().pop_front();
                if let Some(task) = task {
                    //  kwasm::log(&format!("WORKER {:?}: Stealing task!", self.id));
                    task.run(&self);
                    ran_a_task = true;
                    // Only steal a single task before checking my own queue
                    break;
                }
            }

            // If no tasks were available then go to sleep until tasks are available
            if !ran_a_task {
                //   println!("WORKER {:?}: Waiting for tasks to steal", self.id);
                // If I reach here then block until other tasks are enqueued.
                self.new_task.wait();
                // println!("WORKER {:?}: Woken up to steal a task", self.id);
            }
        }
    }

    /*
    pub fn run_task(&mut self) {
        // This is separated into two lines so that self.tasks isn't borrowed while running the task
        let task = self.task_queue.lock().unwrap().pop_front();
        if let Some(task) = task {
            task.run(&self);
        }
    }
    */

    pub fn run_current_thread_tasks(&self) {
        let mut ran_a_task = true;
        while ran_a_task {
            ran_a_task = false;

            // First run tasks from my local queue.
            let task = self.local_task_queue.0.lock().unwrap().pop_front();
            if let Some(task) = task {
                task.run(&self);
                ran_a_task = true;
            }

            // Then run tasks from my local queue that are stealable
            let task = self.task_queue.lock().unwrap().pop_front();
            if let Some(task) = task {
                task.run(&self);
                ran_a_task = true;
            }
        }

        // Wake other workers in case we've created tasks for them.
        self.new_task.wake_one();
    }
}

/// Spawn a task that can be run on any thread.
pub fn spawn<T: Send + 'static>(
    task: impl Future<Output = T> + Send + 'static,
) -> JoinHandle<'static, T> {
    WORKER.with(|w| w.borrow().spawn(task))
}

/// Spawn a task on the current thread
pub fn spawn_local<T: 'static>(task: impl Future<Output = T> + 'static) -> JoinHandle<'static, T> {
    WORKER.with(|w| w.borrow().spawn_local(task))
}

/// Spawn a task on the main thread
pub fn spawn_main<T: Send + 'static>(
    task: impl Future<Output = T> + Send + 'static,
) -> JoinHandle<'static, T> {
    WORKER.with(|w| w.borrow().spawn_main(task))
}

pub fn run_current_thread_tasks() {
    WORKER.with(|w| w.borrow().run_current_thread_tasks())
}

// This needs to store the TaskQueue to push to and the parent task to wake.
// In what scenarios would a waker be cloned?
// Does the parent task need to be stored in an option that's taken when waking the parent?
mod worker_enqueue_waker {
    use super::*;
    use std::sync::Arc;
    use std::task::{RawWaker, RawWakerVTable, Waker};

    struct WakerData<'a> {
        task: Arc<Task<'a>>,
        task_queue: TaskQueue<'a>,
        //worker_waker: WorkerWaker,
    }

    pub(crate) fn create<'a>(
        task: Arc<Task<'a>>,
        task_queue: TaskQueue<'a>,
        _worker_waker: WorkerWaker,
    ) -> Waker {
        let waker_data = Arc::new(WakerData {
            task,
            task_queue,
            //worker_waker,
        });
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

        worker_data
            .task_queue
            .lock()
            .unwrap()
            .push_back(worker_data.task.clone());

        // Run current thread tasks here to run the awoken task.
        // This also means that single-threaded systems will run the task without
        // needing to be workn up.
        run_current_thread_tasks();
    }

    // Do not consume the data pointer
    unsafe fn wake_by_ref(worker_data: *const ()) {
        println!("WAKING BY REF");

        let worker_data = Arc::from_raw(worker_data as *const WakerData);
        //  let task_active = worker_data.task.inner_task.lock().unwrap().is_some();

        //  if task_active {
        worker_data
            .task_queue
            .lock()
            .unwrap()
            .push_back(worker_data.task.clone());
        //  }

        // Is this into_raw call correct
        Arc::into_raw(worker_data);

        // Run current thread tasks here to run the awoken task.
        // This also means that single-threaded systems will run the task without
        // needing to be workn up.
        run_current_thread_tasks();
    }

    unsafe fn drop(worker_data: *const ()) {
        // Convert back to an Arc so that it can be dropped
        let _ = Arc::from_raw(worker_data as *const WakerData);
    }
}

// Almost entirely copy-pated from the above, with small tweaks. This could certainly be improved.
mod worker_enqueue_waker_local {
    use super::*;
    use std::sync::Arc;
    use std::task::{RawWaker, RawWakerVTable, Waker};

    struct WakerData<'a> {
        task: Arc<LocalTask<'a>>,
        task_queue: LocalTaskQueue<'a>,
        worker_waker: WorkerWaker,
    }

    pub(crate) fn create<'a>(
        task: Arc<LocalTask<'a>>,
        task_queue: LocalTaskQueue<'a>,
        worker_waker: WorkerWaker,
    ) -> Waker {
        let waker_data = Arc::new(WakerData {
            task,
            task_queue,
            worker_waker,
        });
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
        // let task_active = worker_data.task.inner_task.is_some();

        // if task_active {
        // println!("TASK ACTIVE");
        worker_data
            .task_queue
            .0
            .lock()
            .unwrap()
            .push_back(worker_data.task.clone()); // Is this clone unnecessary?
                                                  // }
                                                  // worker_data is dropped here.

        // This could run on any thread, so wake all that might be waiting for a task.
        worker_data.worker_waker.wake_all();
    }

    // Do not consume the data pointer
    unsafe fn wake_by_ref(worker_data: *const ()) {
        println!("WAKING BY REF");

        let worker_data = Arc::from_raw(worker_data as *const WakerData);
        //  let task_active = worker_data.task.inner_task.is_some();

        // if task_active {
        worker_data
            .task_queue
            .0
            .lock()
            .unwrap()
            .push_back(worker_data.task.clone());
        // }

        // This could run on any thread, so wake all that might be waiting for a task.
        worker_data.worker_waker.wake_all();

        Arc::into_raw(worker_data);
    }

    unsafe fn drop(worker_data: *const ()) {
        // Convert back to an Arc so that it can be dropped
        let _ = Arc::from_raw(worker_data as *const WakerData);
    }
}
