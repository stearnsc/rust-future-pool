#![feature(fnbox)]

extern crate future;

use future::Future;
use std::boxed::FnBox;
use std::collections::VecDeque;
use std::fmt::Debug;
use std::mem::drop;
use std::sync::mpsc::{channel, Sender, Receiver, RecvError, TryRecvError};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::thread;

type Work = Box<FnBox() -> ()>;

type WorkerId = usize;

pub struct FuturePool {
    workers: Vec<WorkerHandler>,
    availability_rx: Receiver<WorkerId>,
    availability_tx: Sender<WorkerId>,
    work_queue: Arc<Mutex<VecDeque<Work>>>
}

struct WorkerHandler {
    join_handle: JoinHandle<()>,
    wake_tx: Sender<Message>
}

impl FuturePool {
    pub fn new(size: usize) -> FuturePool {
        let work_queue = Arc::new(Mutex::new(VecDeque::new()));
        let mut workers = Vec::with_capacity(size);

        let (availability_tx, availability_rx) = channel();

        for thread_id in 0..size {
            workers.push(Worker::spawn(thread_id, &work_queue, availability_tx.clone()));
        }

        FuturePool {
            workers: workers,
            availability_rx: availability_rx,
            availability_tx: availability_tx,
            work_queue: work_queue
        }
    }

    pub fn thread_count(&self) -> usize {
        self.workers.len()
    }

    pub fn run<F, A, E>(&self, f: F) -> Future<A, E>
        where F: FnOnce() -> Result<A, E> + 'static,
              A: Debug + 'static, E: Debug + 'static
    {
        let (future, setter) = future::new();
        self.add_work(Box::new(|| setter.set_result(f())));
        future
    }

    pub fn resize(&mut self, new_thread_count: usize) {
        let current_size = self.thread_count();
        if new_thread_count > current_size {
            for worker_id in current_size..new_thread_count {
                self.workers.push(
                    Worker::spawn(worker_id, &self.work_queue, self.availability_tx.clone())
                );
            }
        } else if new_thread_count < current_size {
            for worker_id in new_thread_count..current_size {
                let worker = self.workers.remove(worker_id);
                let _ = worker.wake_tx.send(Message::Shutdown);
            }
        }
    }

    fn add_work(&self, work: Work) {
        let mut work_queue = self.work_queue.lock().unwrap();
        (*work_queue).push_back(work);
        drop(work_queue);
        match self.next_available_worker() {
            Some(waiting_worker_id) =>
                self.workers[waiting_worker_id].wake_tx.send(Message::StartWork)
                    .expect("Error communicating with worker threads!"),
            None => {}
        }
    }

    fn next_available_worker(&self) -> Option<WorkerId> {
        let num_workers = self.thread_count();
        if num_workers == 0 {
            None
        } else {
            loop {
                match self.availability_rx.try_recv() {
                    Ok(id) if id < num_workers => return Some(id),
                    Ok(_) => {}, //worker shut down, keep looping
                    Err(TryRecvError::Empty) => return None,
                    Err(TryRecvError::Disconnected) => panic!("Error communicating with workers!")
                }
            }
        }
    }
}

struct Worker {
    worker_id: WorkerId,
    work_queue: Arc<Mutex<VecDeque<Work>>>,
    availability_tx: Sender<WorkerId>,
    wake_rx: Receiver<Message>
}

impl Worker {
    fn spawn(
        worker_id: WorkerId,
        work_queue: &Arc<Mutex<VecDeque<Work>>>,
        availability_tx: Sender<WorkerId>
    ) -> WorkerHandler {
        let (wake_tx, wake_rx) = channel();
        let worker = Worker {
            worker_id: worker_id,
            work_queue: work_queue.clone(),
            availability_tx: availability_tx,
            wake_rx: wake_rx
        };

        let join_handle = thread::spawn(move || worker.run());

        WorkerHandler {
            join_handle: join_handle,
            wake_tx: wake_tx
        }
    }

    fn run(self) {
        loop {
            let mut queue = self.work_queue.lock().unwrap();
            match (*queue).pop_front() {
                Some(work) => {
                    drop(queue);
                    work();
                },
                None => {
                    match self.availability_tx.send(self.worker_id) {
                        Err(_) => break, //thread pool has been dropped, shut down
                        _ => {}
                    }

                    drop(queue);
                    match self.wake_rx.recv() {
                        Ok(Message::StartWork) => {}, //continue looping
                        Ok(Message::Shutdown) |
                        Err(RecvError) => break
                    }
                }
            }
        }
    }
}

unsafe impl Send for Worker {}

#[derive(Clone, Copy, Debug)]
enum Message {
    StartWork,
    Shutdown
}

#[cfg(test)]
mod test {
    #[test]
    fn it_works() {
    }
}
