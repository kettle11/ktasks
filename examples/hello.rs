use ktasks::*;
use std::task::Poll;

fn main() {
    //let mut worker = Worker::new();
    create_workers(2);

    let task0 = spawn(|| {
        println!("HERE IN TASK0");
        std::thread::sleep(std::time::Duration::from_millis(100));
        100
    });
    println!("SECOND TASK ADDING");
    let task1 = spawn(|| {
        //std::thread::sleep(std::time::Duration::from_millis(1000));
        println!("HERE IN TASK1");

        spawn(|| {
            println!("A NESTED TASK--------------------------");
        });
        300
    });

    let task2 = spawn(|| {
        //std::thread::sleep(std::time::Duration::from_millis(1000));
        println!("HERE IN TASK2");
        300
    });

    let task3 = spawn(|| {
        //std::thread::sleep(std::time::Duration::from_millis(1000));
        println!("HERE IN TASK3");
        300
    });

    let task4 = spawn(|| {
        //std::thread::sleep(std::time::Duration::from_millis(1000));
        println!("HERE IN TASK4");
        300
    });

    std::thread::sleep(std::time::Duration::from_millis(3000));
    run_tasks();
    /*
    if let Poll::Ready(result) = task0.is_complete() {
        println!("RESULT: {:?}", result);
    }
    if let Poll::Ready(result) = task1.is_complete() {
        println!("RESULT: {:?}", result);
    }
    */
}
