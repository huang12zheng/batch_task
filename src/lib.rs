#![feature(impl_trait_in_bindings)]
#![feature(type_alias_impl_trait)]

/// #[cfg(test)]
/// async fn run(max_batch_size: usize, shutdown_flag: bool) -> Result<()> {
///     let processor: impl Fn(Vec<Box<dyn IItem>>) -> ProcessorOutput = |item| {
///         Box::pin(async move {
///             dbg!(item.len());
///             for e in item {
///                 dbg!(&e.encode());
///             }
///             Ok(())
///         })
///     };
///
///     let (writer, handle) = BatchWriter::new(Box::new(processor), None, Some(max_batch_size), None);
///     writer.add_item(4);
///     writer.add_item(5);
///     tokio::time::sleep(Duration::from_secs(1)).await;
///     if shutdown_flag {
///         writer.shutdown().await;
///         let _ = handle.await;
///     }
///     Ok(())
/// }
pub use std::{pin::Pin, sync::Arc, time::Duration};

pub use anyhow::{Ok, Result};
pub use bytes::Bytes;
use tokio::sync::mpsc::channel;
pub use tokio::sync::mpsc::{Receiver, Sender};
pub use tokio::task::JoinHandle;
use tokio::{task, time::sleep};
mod _trait {
    use bytes::Bytes;

    pub trait IItem: Send + Sync {
        fn encode(&self) -> Bytes;
    }
}
pub use _trait::*;

// pub type BatchProcessor = impl Fn(Vec<impl IItem>) -> Pin<Box<dyn Future<Output = Result<()>>>>;
pub type ProcessorOutput = Pin<Box<dyn Future<Output = Result<()>> + Send>>;
// pub type BatchProcessor = impl Fn(Vec<Arc<dyn IItem>>) -> Pin<Box<dyn Future<Output = Result<()>>>>;

pub struct BatchWriter {
    item_sender: Sender<Box<dyn IItem>>,
    flush_sender: Sender<()>,
}

impl BatchWriter {
    pub fn new(
        processor: impl Fn(Vec<Box<dyn IItem>>) -> ProcessorOutput + Sync + Send + 'static,
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
            async move {
                BatchWriter::batch_processor_task(
                    item_receiver,
                    flush_receiver,
                    max_batch_size.unwrap_or(10),
                    batch_processor,
                )
                .await;
            }
        });

        // 启动定时刷新任务
        let interval_sender = flush_sender.clone();
        tokio::spawn(async move {
            loop {
                sleep(interval.unwrap_or(Duration::from_secs(30))).await;
                let _ = interval_sender.send(());
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
    pub fn add_item(&self, item: impl IItem + 'static) {
        // 非阻塞发送，如果队列满则丢弃数据（可根据需求调整策略）
        let _ = self.item_sender.try_send(Box::new(item));
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
        mut data_receiver: Receiver<Box<dyn IItem>>,
        mut flush_receiver: Receiver<()>,
        max_batch_size: usize,
        batch_processor: Arc<
            impl Fn(Vec<Box<dyn IItem>>) -> ProcessorOutput + Sync + Send + 'static,
        >,
    ) {
        let mut buffer = Vec::with_capacity(max_batch_size);
        dbg!(107);
        loop {
            // 使用crossbeam的select!同时等待数据和刷新信号
            tokio::select! {
                item = data_receiver.recv() => {
                    match item {
                        Some(item) => {
                            dbg!(114);
                            buffer.push(item);

                            // 达到批量大小时立即处理
                            if buffer.len() >= max_batch_size {
                                let batch = std::mem::replace(&mut buffer, Vec::with_capacity(max_batch_size));
                                if let Err(e) = (*batch_processor)(batch).await {
                                    eprintln!("Batch processing error: {}", e);
                                }
                            }
                        }
                        None => {
                            dbg!(125);
                            break
                        }, // 通道关闭时退出
                    }
                },
                _ = flush_receiver.recv() => {
                    dbg!(buffer.len());
                    if !buffer.is_empty() {
                        let batch = std::mem::replace(&mut buffer, Vec::with_capacity(max_batch_size));
                        if let Err(e) = (*batch_processor)(batch).await {
                            eprintln!("Flush processing error: {}", e);
                        }
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
impl IItem for u64 {
    fn encode(&self) -> Bytes {
        Bytes::from(self.to_string())
    }
}

#[tokio::test]
async fn main_write2cycle() -> Result<()> {
    run(1, false).await
}
#[tokio::test]
async fn main_nowrite() -> Result<()> {
    run(3, false).await
}
#[tokio::test]
async fn main_shutdown() -> Result<()> {
    run(3, true).await
}
#[tokio::test]
async fn main_write2() -> Result<()> {
    run(2, false).await
}
#[cfg(test)]
async fn run(max_batch_size: usize, shutdown_flag: bool) -> Result<()> {
    let processor: impl Fn(Vec<Box<dyn IItem>>) -> ProcessorOutput = |item| {
        Box::pin(async move {
            dbg!(item.len());
            for e in item {
                dbg!(&e.encode());
            }
            Ok(())
        })
    };

    let (writer, handle) = BatchWriter::new(Box::new(processor), None, Some(max_batch_size), None);
    writer.add_item(4);
    writer.add_item(5);
    tokio::time::sleep(Duration::from_secs(1)).await;
    if shutdown_flag {
        writer.shutdown().await;
        let _ = handle.await;
    }
    Ok(())
}
