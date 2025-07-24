use anyhow::Result;
use async_nats::Client;

pub async fn connect(nats_url: &str) -> Result<Client> {
    let nc = async_nats::connect(nats_url).await?;
    Ok(nc)
}

pub async fn subscribe(nc: &Client, subject: &str) -> Result<async_nats::Subscriber> {
    let sub = nc.subscribe(subject.to_string()).await?;
    Ok(sub)
}
