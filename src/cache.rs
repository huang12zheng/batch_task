use dashmap::DashMap;
use std::{
    any::{Any, TypeId},
    sync::{Arc, OnceLock, Weak},
};
use tokio::sync::broadcast;

use crate::{BatchWriter, Callback, add2pool};

struct BatchWriterCache {
    instances: Arc<DashMap<TypeId, Box<dyn Any + Send + Sync>>>,
    pub(crate) shutdown_tx: broadcast::Sender<()>,
}

// impl BatchWriterCache<>

impl BatchWriterCache {
    fn global() -> &'static Self {
        static CACHE: OnceLock<BatchWriterCache> = OnceLock::new();
        CACHE.get_or_init(|| {
            let (shutdown_tx, _) = broadcast::channel(1);
            BatchWriterCache {
                instances: Arc::new(DashMap::new()),
                shutdown_tx,
            }
        })
    }

    fn get_or_create<T: Callback + 'static>(&self) -> Arc<BatchWriter<T>> {
        let type_id = TypeId::of::<T::Item>();

        if let Some(existing) = self.instances.get(&type_id) {
            if let Some(weak_tuple) = existing.downcast_ref::<WeakTuple<BatchWriter<T>>>() {
                if let Some(arc_writer) = (&weak_tuple.0).upgrade() {
                    return arc_writer;
                }
            }
        }

        let shutdown_rx = self.shutdown_tx.subscribe(); // 创建订阅
        // 创建新实例
        let (writer, handle) = BatchWriter::<T>::new(None, None, None, shutdown_rx);
        add2pool(handle);
        let writer_arc = Arc::new(writer);
        let weak_ref = Arc::downgrade(&writer_arc);

        // 存储弱引用和句柄的克隆
        let weak_tuple = WeakTuple(weak_ref);
        self.instances.insert(type_id, Box::new(weak_tuple));

        writer_arc
    }
}
pub fn shutdown_all() {
    let _ = BatchWriterCache::global().shutdown_tx.send(());
}

// 用于存储弱引用和任务句柄的辅助结构
struct WeakTuple<T: ?Sized>(Weak<T>);

impl<T: Callback + 'static> BatchWriter<T> {
    pub fn none() -> Arc<Self> {
        BatchWriterCache::global().get_or_create::<T>()
    }
}
