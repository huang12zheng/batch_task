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
    pub trait Callback: Send + Sync {
        type Item: Send + 'static;
        fn callback(&self) -> anyhow::Result<()>;
    }
}
pub use _trait::*;

pub type ProcessorOutput = Pin<Box<dyn Future<Output = Result<()>> + Send>>;

pub struct BatchWriter<T: Callback> {
    item_sender: Sender<T::Item>,
    flush_sender: Sender<()>,
}

impl<T: Callback> BatchWriter<T> {
    pub fn new(
        processor: impl Fn(Vec<T::Item>) -> ProcessorOutput + Sync + Send + 'static,
        interval: Option<Duration>,
        max_batch_size: Option<usize>,
        buffer_size: Option<usize>,
    ) -> (Self, task::JoinHandle<()>) {
        // 创建跨线程通道
        let (item_sender, item_receiver) = channel(buffer_size.unwrap_or(1024));
        let (flush_sender, flush_receiver) = channel(1);

        // 使用Arc共享批次处理器
        let batch_processor = Arc::new(processor);

        // 启动Tokio异步写入任务
        let handle: JoinHandle<()> = tokio::spawn({
            let batch_processor = batch_processor.clone();
            let flush_sender_ = flush_sender.clone();
            async move {
                Self::batch_processor_task(
                    item_receiver,
                    flush_sender_,
                    flush_receiver,
                    max_batch_size.unwrap_or(10),
                    interval.unwrap_or(Duration::from_secs(30)),
                    batch_processor,
                )
                .await;
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
        // 发送刷新信号（翻转布尔值）
        let _ = self.flush_sender.send(()).await;
    }

    /// 手动触发刷新
    pub async fn shutdown(self) {
        drop(self.item_sender);
        drop(self.flush_sender);
    }

    /// 异步任务核心：处理数据并调用批次处理器
    async fn batch_processor_task(
        mut data_receiver: Receiver<T::Item>,
        flush_sender: Sender<()>,
        mut flush_receiver: Receiver<()>,
        max_batch_size: usize,
        interval: Duration,
        batch_processor: Arc<impl Fn(Vec<T::Item>) -> ProcessorOutput + Sync + Send + 'static>,
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
                                if let Err(e) = (*batch_processor)(batch).await {
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
                    let _ = flush_sender.send(());
                }
                _ = flush_receiver.recv() => {
                    dbg!(buffer.len());
                    if !buffer.is_empty() {
                        let batch = std::mem::replace(&mut buffer, Vec::with_capacity(max_batch_size));
                        if let Err(e) = (*batch_processor)(batch).await {
                            eprintln!("Flush processing error: {}", e);
                        }
                    } else {
                        delay *= 2
                    }
                }
            }
        }
        dbg!(137);
        // 退出时处理剩余数据
        if !buffer.is_empty() {
            let batch = std::mem::replace(&mut buffer, Vec::new());
            if let Err(e) = (*batch_processor)(batch).await {
                eprintln!("Final batch processing error: {}", e);
            }
        }
    }
}
#[cfg(test)]
impl Callback for Vec<u64> {
    type Item = u64;
    fn callback(&self) -> Result<()> {
        let mut v = RESULT.lock().unwrap();
        v.push(self.len().to_string());

        for e in self {
            v.push(e.to_string())
        }
        Ok(())
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
    let processor: impl Fn(Vec<u64>) -> ProcessorOutput = |item| {
        Box::pin(async move {
            let _ = item.callback()?;
            Ok(())
        })
    };

    let (writer, handle) =
        BatchWriter::<Vec<u64>>::new(Box::new(processor), None, Some(max_batch_size), None);
    writer.add_item(4);
    writer.add_item(5);
    tokio::time::sleep(Duration::from_secs(1)).await;
    if shutdown_flag {
        writer.shutdown().await;
        let _ = handle.await;
    }

    Ok(())
}
