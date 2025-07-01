use std::sync::{Arc, OnceLock};

use std::sync::Mutex;

use tokio::task::JoinHandle;

fn join_handle() -> Arc<Mutex<Vec<JoinHandle<()>>>> {
    static HANDLES: OnceLock<Arc<Mutex<Vec<JoinHandle<()>>>>> = OnceLock::new();
    HANDLES.get_or_init(|| Arc::new(Mutex::new(vec![]))).clone()
}

pub(crate) fn add2pool(handle: JoinHandle<()>) {
    join_handle().lock().unwrap().push(handle);
}
pub async fn finish_pool() {
    let binding = join_handle();
    let handles = std::mem::take(&mut *binding.lock().unwrap());

    let _ = futures::future::join_all(handles).await;
}
