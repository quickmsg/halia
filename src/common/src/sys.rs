use sysinfo::{Disks, System};
use types::MachineInfo;

pub fn load_info() -> MachineInfo {
    let mut sys = System::new_all();
    sys.refresh_cpu_all();
    sys.refresh_memory();

    let pid = sysinfo::get_current_pid().unwrap();
    let halia_memory = sys.process(pid).unwrap().memory();

    let disks = Disks::new_with_refreshed_list();
    let mut disk_infos = Vec::with_capacity(disks.len());
    for disk in &disks {
        disk_infos.push((
            disk.name().to_str().unwrap().to_owned(),
            disk.total_space(),
            disk.available_space(),
        ));
    }

    MachineInfo {
        total_memory: sys.total_memory(),
        used_memory: sys.used_memory(),
        halia_memory,
        global_cpu_usage: sys.global_cpu_usage(),
        disks: disk_infos,
    }
}
