#![feature(impl_trait_in_bindings)]
#[cfg(test)]
use std::sync::Mutex;

pub use std::{pin::Pin, sync::Arc, time::Duration};

pub use anyhow::{Ok, Result};

use tokio::sync::mpsc::channel;
pub use tokio::sync::mpsc::{Receiver, Sender};
pub use tokio::task::JoinHandle;
use tokio::{task, time::sleep};
mod _trait {
    use std::fmt::Debug;
    pub trait Callback: Send + Sync {
        type Item: Send + Debug;
        fn callback(self) -> impl std::future::Future<Output = anyhow::Result<()>> + Send;
        fn inner(&self) -> &[Self::Item];
        fn outer(value: Vec<Self::Item>) -> Self;
    }
}
pub use _trait::*;
mod join_handle;
pub use join_handle::*;
mod cache;
pub use cache::shutdown_all;

struct FlushRequest {
    resp: tokio::sync::oneshot::Sender<()>,
}

pub struct BatchWriter<T: Callback> {
    item_sender: Sender<T::Item>,
    flush_sender: Sender<FlushRequest>,
}

impl<T: Callback + 'static> BatchWriter<T> {
    pub fn new(
        interval: Option<Duration>,
        max_batch_size: Option<usize>,
        buffer_size: Option<usize>,
        shutdown_rx: Option<tokio::sync::broadcast::Receiver<()>>,
    ) -> (Self, task::JoinHandle<()>) {
        // 创建跨线程通道
        let (item_sender, item_receiver) = channel(buffer_size.unwrap_or(1024));
        let (flush_sender, flush_receiver) = channel(1);

        // 启动Tokio异步写入任务
        let handle: JoinHandle<()> = tokio::spawn({
            async move {
                if let Some(shutdown_rx) = shutdown_rx {
                    Self::batch_processor_task(
                        item_receiver,
                        flush_receiver,
                        shutdown_rx,
                        max_batch_size.unwrap_or(10),
                        interval.unwrap_or(Duration::from_secs(30)),
                    )
                    .await;
                } else {
                    Self::batch_processor_task_base(
                        item_receiver,
                        flush_receiver,
                        max_batch_size.unwrap_or(10),
                        interval.unwrap_or(Duration::from_secs(30)),
                    )
                    .await;
                }
            }
        });

        (
            Self {
                item_sender,
                flush_sender,
            },
            handle,
        )
    }

    /// 添加数据对象到批处理队列
    pub fn add_item(&self, item: T::Item) {
        // 非阻塞发送，如果队列满则丢弃数据（可根据需求调整策略）
        let _ = self.item_sender.try_send(item);
    }

    pub async fn flush(&self) {
        let (resp_tx, resp_rx) = tokio::sync::oneshot::channel();
        // 发送刷新信号（翻转布尔值）
        let _ = self.flush_sender.send(FlushRequest { resp: resp_tx }).await;
        let _ = resp_rx.await;
    }

    /// 手动触发刷新
    pub async fn shutdown(self) {
        drop(self.item_sender);
        drop(self.flush_sender);
    }

    /// 异步任务核心：处理数据并调用批次处理器
    async fn batch_processor_task(
        mut data_receiver: Receiver<T::Item>,
        mut flush_receiver: Receiver<FlushRequest>,
        mut shutdown_rx: tokio::sync::broadcast::Receiver<()>,
        max_batch_size: usize,
        interval: Duration,
    ) {
        let mut buffer = Vec::with_capacity(max_batch_size);
        let mut delay = interval.clone();
        loop {
            tokio::select! {
                item = data_receiver.recv() => {
                    match item {
                        Some(item) => {
                            buffer.push(item);

                            // 达到批量大小时立即处理
                            if buffer.len() >= max_batch_size {
                                let batch = std::mem::replace(&mut buffer, Vec::with_capacity(max_batch_size));
                                if let Err(e) = (T::callback)(T::outer(batch)).await {
                                    eprintln!("Batch processing error: {}", e);
                                }
                            }
                            delay = interval
                        }
                        None => {
                            break
                        }, // 通道关闭时退出
                    }
                },
                _ = sleep(delay) => {
                    if !buffer.is_empty() {
                        let batch = std::mem::replace(&mut buffer, Vec::with_capacity(max_batch_size));
                        if let Err(e) = (T::callback)(T::outer(batch)).await {
                            eprintln!("Flush processing error: {}", e);
                        }
                    } else {
                        delay *= 2
                    }
                }
                flush_req = flush_receiver.recv() => {
                    if let Some(resp) = flush_req {
                        if !buffer.is_empty() {
                            let batch = std::mem::replace(&mut buffer, Vec::with_capacity(max_batch_size));
                            if let Err(e) = (T::callback)(T::outer(batch)).await {
                                eprintln!("Flush processing error: {}", e);
                            }
                        }
                        let _ = resp.resp.send(());
                    }
                    else {
                        break;
                    }

                }
                _ = shutdown_rx.recv() => {
                    break
                }
            }
        }

        // 退出时处理剩余数据
        if !buffer.is_empty() {
            let batch = std::mem::replace(&mut buffer, Vec::new());
            if let Err(e) = (T::callback)(T::outer(batch)).await {
                eprintln!("Final batch processing error: {}", e);
            }
        }
    }
    async fn batch_processor_task_base(
        mut data_receiver: Receiver<T::Item>,
        mut flush_receiver: Receiver<FlushRequest>,
        max_batch_size: usize,
        interval: Duration,
    ) {
        let mut buffer = Vec::with_capacity(max_batch_size);
        let mut delay = interval.clone();
        loop {
            tokio::select! {
                item = data_receiver.recv() => {
                    match item {
                        Some(item) => {
                            buffer.push(item);

                            // 达到批量大小时立即处理
                            if buffer.len() >= max_batch_size {
                                let batch = std::mem::replace(&mut buffer, Vec::with_capacity(max_batch_size));
                                if let Err(e) = (T::callback)(T::outer(batch)).await {
                                    eprintln!("Batch processing error: {}", e);
                                }
                            }

                            dbg!(std::time::SystemTime::now());
                            delay = interval;
                        }
                        None => {
                            break
                        }, // 通道关闭时退出
                    }
                },
                _ = sleep(delay) => {
                    if !buffer.is_empty() {
                        let batch = std::mem::replace(&mut buffer, Vec::with_capacity(max_batch_size));
                        if let Err(e) = (T::callback)(T::outer(batch)).await {
                            eprintln!("Flush processing error: {}", e);
                        }
                    } else {
                        delay *= 2
                    }
                }
                flush_req = flush_receiver.recv() => {
                    if let Some(resp) = flush_req {
                        if !buffer.is_empty() {
                            let batch = std::mem::replace(&mut buffer, Vec::with_capacity(max_batch_size));
                            if let Err(e) = (T::callback)(T::outer(batch)).await {
                                eprintln!("Flush processing error: {}", e);
                            }
                        }
                        let _ = resp.resp.send(());
                    }
                    else {
                        break;
                    }
                }
            }
        }

        // 退出时处理剩余数据
        if !buffer.is_empty() {
            let batch = std::mem::replace(&mut buffer, Vec::new());
            if let Err(e) = (T::callback)(T::outer(batch)).await {
                eprintln!("Final batch processing error: {}", e);
            }
        }
    }
}
#[cfg(test)]
pub struct U64List(Vec<u64>);
#[cfg(test)]
impl Callback for U64List {
    type Item = u64;
    async fn callback(self) -> Result<()> {
        let mut v = RESULT.lock().unwrap();
        v.push(self.inner().len().to_string());

        for e in self.inner() {
            v.push(e.to_string())
        }
        Ok(())
    }

    fn inner(&self) -> &[Self::Item] {
        &self.0
    }

    fn outer(value: Vec<Self::Item>) -> Self {
        Self(value)
    }
}

#[tokio::test]
async fn main_write2cycle() {
    run(1, false).await.unwrap();
    let mut v = RESULT.lock().unwrap();
    insta::assert_debug_snapshot!(v,@r#"
    [
        "1",
        "4",
        "1",
        "5",
    ]
    "#);
    v.clear();
}
#[tokio::test]
async fn main_nowrite() {
    run(3, false).await.unwrap();
    let mut v = RESULT.lock().unwrap();
    insta::assert_debug_snapshot!(v,@"[]");
    v.clear();
}
#[tokio::test]
async fn main_shutdown() {
    run(3, true).await.unwrap();
    let mut v = RESULT.lock().unwrap();
    insta::assert_debug_snapshot!(v,@r#"
    [
        "2",
        "4",
        "5",
    ]
    "#);
    v.clear();
}
#[tokio::test]
async fn main_write2() {
    run(2, false).await.unwrap();
    let mut v = RESULT.lock().unwrap();
    insta::assert_debug_snapshot!(v,@r#"
    [
        "2",
        "4",
        "5",
    ]
    "#);
    v.clear();
}
#[cfg(test)]
pub static RESULT: std::sync::Mutex<Vec<String>> = Mutex::new(vec![]);
#[cfg(test)]
async fn run(max_batch_size: usize, shutdown_flag: bool) -> Result<()> {
    let (writer, handle) = BatchWriter::<U64List>::new(None, Some(max_batch_size), None, None);
    writer.add_item(4);
    writer.add_item(5);
    tokio::time::sleep(Duration::from_secs(1)).await;
    if shutdown_flag {
        writer.shutdown().await;
        let _ = handle.await;
    }

    Ok(())
}
