use sysinfo::{Disks, System};

pub fn load_info() {
    let mut sys = System::new_all();
    sys.refresh_cpu_all();
    sys.refresh_memory();
    println!("=> system:");
    println!(
        "total memory: {} GB",
        format!(
            "{:.2}",
            sys.total_memory() as f64 / (1024 * 1024 * 1024) as f64
        ),
    );
    println!(
        "used memory : {} GB",
        format!(
            "{:.2}",
            sys.used_memory() as f64 / (1024 * 1024 * 1024) as f64
        ),
    );

    let pid = sysinfo::get_current_pid().unwrap();
    if let Some(process) = sys.process(pid) {
        println!(
            "Memory usage: {} GB",
            format!(
                "{:.2}",
                (process.memory() as f64) / ((1024 * 1024 * 1024) as f64)
            )
        );
    }
    println!("Total CPU usage: {:.2}%", sys.global_cpu_usage() * 100.0);
    let disks = Disks::new_with_refreshed_list();
    for disk in &disks {
        println!(
            "{}",
            format!(
                "{:.2}",
                (disk.available_space() as f64) / ((1024 * 1024 * 1024) as f64)
            )
        );
        println!(
            "{}",
            format!(
                "{:.2}",
                (disk.total_space() as f64) / ((1024 * 1024 * 1024) as f64)
            )
        );
    }
}
