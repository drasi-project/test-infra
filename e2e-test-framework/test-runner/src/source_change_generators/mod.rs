use async_trait::async_trait;

// pub mod change_script_player;

#[derive(Debug, thiserror::Error)]
pub enum SourceChangeGeneratorError {
}

#[derive(Debug)]
pub struct SourceChangeGeneratorCommandResponse {
    pub result: anyhow::Result<()>,
    pub state: SourceChangeGeneratorState,
}

#[derive(Debug)]
pub struct SourceChangeGeneratorState {    
}

#[async_trait]
pub trait SourceChangeGenerator : Send + Sync {
    async fn get_state(&self) -> anyhow::Result<SourceChangeGeneratorCommandResponse>;

    async fn start(&self) -> anyhow::Result<SourceChangeGeneratorCommandResponse>;

    async fn step(&self, steps: u64) -> anyhow::Result<SourceChangeGeneratorCommandResponse>;

    async fn skip(&self, skips: u64) -> anyhow::Result<SourceChangeGeneratorCommandResponse>;

    async fn pause(&self) -> anyhow::Result<SourceChangeGeneratorCommandResponse>;

    async fn stop(&self) -> anyhow::Result<SourceChangeGeneratorCommandResponse>;
}

#[async_trait]
impl SourceChangeGenerator for Box<dyn SourceChangeGenerator + Send + Sync> {
    async fn get_state(&self) -> anyhow::Result<SourceChangeGeneratorCommandResponse> {
        (**self).get_state().await
    }

    async fn start(&self) -> anyhow::Result<SourceChangeGeneratorCommandResponse> {
        (**self).start().await
    }

    async fn step(&self, steps: u64) -> anyhow::Result<SourceChangeGeneratorCommandResponse> {
        (**self).step(steps).await
    }

    async fn skip(&self, skips: u64) -> anyhow::Result<SourceChangeGeneratorCommandResponse> {
        (**self).skip(skips).await
    }

    async fn pause(&self) -> anyhow::Result<SourceChangeGeneratorCommandResponse> {
        (**self).pause().await
    }

    async fn stop(&self) -> anyhow::Result<SourceChangeGeneratorCommandResponse> {
        (**self).stop().await
    }
}