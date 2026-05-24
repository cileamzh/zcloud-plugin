// lib.rs
use prost::Message;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tokio::net::UnixStream;

// 统一的底层异步二进制闭包类型（彻底解耦掉特定的文本格式）
type BoxedAsyncMethodHandler = Arc<
    dyn Fn(Vec<u8>) -> Pin<Box<dyn Future<Output = Result<Vec<u8>, String>> + Send>> + Send + Sync,
>;
type BoxedAsyncStreamHandler =
    Arc<dyn Fn(UnixStream, Vec<u8>) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>;

pub struct ZPlugin {
    method_handlers: HashMap<String, BoxedAsyncMethodHandler>,
    stream_handlers: HashMap<String, BoxedAsyncStreamHandler>,
}

impl ZPlugin {
    pub fn new() -> Self {
        Self {
            method_handlers: HashMap::new(),
            stream_handlers: HashMap::new(),
        }
    }

    

    /// 注册跨语言、Protobuf 强类型的异步命令方法
    pub fn register_method<P, R, F, Fut>(&mut self, name: &str, handler: F)
    where
        P: Message + Default + 'static, // 入参必须是 Protobuf 消息
        R: Message + Default + 'static, // 出参必须是 Protobuf 消息
        F: Fn(P) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<R, String>> + Send + 'static,
    {
        let handler_arc = Arc::new(handler);

        // 底层闭包：只认二进制字节流，在进入开发者逻辑前进行拦截转换
        let wrapped_handler = move |raw_bytes: Vec<u8>| {
            let handler_clone = Arc::clone(&handler_arc);
            Box::pin(async move {
                // 1. 极速解码二进制入参 (Protobuf 反序列化)
                let typed_params = P::decode(&*raw_bytes).map_err(|e| {
                    format!(
                        "[SDK ERROR] Failed to decode protobuf request parameters: {}",
                        e
                    )
                })?;

                // 2. 执行核心异步业务
                match handler_clone(typed_params).await {
                    // 3. 极速编码出参为二进制 (Protobuf 序列化)
                    Ok(success_res) => {
                        let mut buf = Vec::new();
                        success_res.encode(&mut buf).map_err(|e| {
                            format!(
                                "[SDK ERROR] Failed to encode protobuf response results: {}",
                                e
                            )
                        })?;
                        Ok(buf)
                    }
                    Err(biz_err) => Err(biz_err),
                }
            }) as Pin<Box<dyn Future<Output = Result<Vec<u8>, String>> + Send>>
        };

        self.method_handlers
            .insert(name.to_string(), Arc::new(wrapped_handler));
    }

    /// 注册跨语言、Protobuf 强类型的异步流处理器
    pub fn register_stream_handler<M, F, Fut>(&mut self, name: &str, handler: F)
    where
        M: Message + Default + 'static, // 元数据必须是 Protobuf 消息
        F: Fn(UnixStream, M) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let handler_arc = Arc::new(handler);

        let wrapped_handler = move |stream: UnixStream, raw_bytes: Vec<u8>| {
            let handler_clone = Arc::clone(&handler_arc);
            Box::pin(async move {
                // 流建立连接时，自动反序列化 Meta 原始二进制
                match M::decode(&*raw_bytes) {
                    Ok(typed_meta) => {
                        handler_clone(stream, typed_meta).await;
                    }
                    Err(e) => {
                        eprintln!(
                            "[SDK ERROR] Failed to decode protobuf stream metadata: {:?}",
                            e
                        );
                    }
                }
            }) as Pin<Box<dyn Future<Output = ()> + Send>>
        };

        self.stream_handlers
            .insert(name.to_string(), Arc::new(wrapped_handler));
    }
}
