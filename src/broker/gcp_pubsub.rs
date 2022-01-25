//! GCP Pubsub broker.

use super::{Broker, BrokerBuilder};
use crate::error::{BrokerError, ProtocolError};
use crate::protocol::{self, Message, TryDeserializeMessage};
use async_trait::async_trait;
use base64;
use chrono::{DateTime, Utc};
use futures::{
    task::{Context, Poll},
    Stream,
};
use log::{error, info, warn};
use reqwest::header;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashMap;
use std::fs::File;
use std::future::Future;
use std::io::{BufReader, ErrorKind};
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::Arc;
use std::task::Waker;
use thiserror::Error;
use uuid::Uuid;

const MESSAGE_TOPIC_NOT_EXIST: &str = "There was an error with the following topic";

// Internal pubsub message
#[derive(Serialize, Deserialize)]
struct PubsubMessage {
    data: String,

    #[serde(rename(deserialize = "messageId"))]
    message_id: String,

    #[serde(rename(deserialize = "publishTime"))]
    publish_time: String,
}

#[derive(Serialize, Deserialize)]
struct ReceivedMessage {
    #[serde(rename(deserialize = "ackId"))]
    ack_id: String,

    #[serde(rename(deserialize = "deliveryAttempt"))]
    delivery_attempt: Option<u32>,

    message: PubsubMessage,
}

#[derive(Serialize, Deserialize)]
struct PubsubPullResponse {
    #[serde(rename(deserialize = "receivedMessages"))]
    received_messages: Option<Vec<ReceivedMessage>>,
}

//
// Broker configuration structs.
//

// General
#[derive(Deserialize, Clone, Debug)]
struct BrokerOptions {
    producer: Option<BrokerProducerOptions>,

    consumer: Option<BrokerConsumerOptions>,
}

impl Default for BrokerOptions {
    fn default() -> Self {
        Self {
            producer: Some(BrokerProducerOptions::default()),
            consumer: Some(BrokerConsumerOptions::default()),
        }
    }
}

#[derive(Deserialize)]
struct GCPErrorMessage {
    error: GCPErrorDetail,
}

#[derive(Deserialize)]
struct GCPErrorDetail {
    message: String,
}

// Producer
#[derive(Deserialize, Clone, Debug)]
struct BrokerProducerOptions {
    create_topic: Option<bool>,
}

impl Default for BrokerProducerOptions {
    fn default() -> Self {
        Self {
            create_topic: Some(true),
        }
    }
}

// Consumer
#[derive(Deserialize, Clone, Debug)]
struct BrokerConsumerOptions {
    create_default_subscription: Option<bool>,
    topics: Option<BrokerTopicOptions>,
}

impl Default for BrokerConsumerOptions {
    fn default() -> Self {
        Self {
            create_default_subscription: Some(false),
            topics: None,
        }
    }
}

type BrokerTopicOptions = HashMap<String, BrokerSubscription>;

#[derive(Deserialize, Serialize, Clone, Debug)]
struct BrokerSuscriptionExpirationPolicy {
    ttl: String,
}

#[derive(Deserialize, Serialize, Clone, Debug)]
#[serde(rename_all(serialize = "camelCase"))]
struct BrokerSuscriptionDeadLetterPolicy {
    dead_letter_topic: String,

    max_delivery_attempts: u8,
}

#[derive(Deserialize, Serialize, Clone, Debug)]
#[serde(rename_all(serialize = "camelCase"))]
struct BrokerSuscriptionRetryPolicy {
    minimum_backoff: String,

    maximum_backoff: String,
}

#[derive(Deserialize, Serialize, Clone, Debug, Default)]
#[serde(rename_all(serialize = "camelCase"))]
struct BrokerSubscription {
    #[serde(rename(deserialize = "subscription_name"), skip_serializing)]
    name: String,

    #[serde(skip_deserializing)]
    topic: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    ack_deadline_seconds: Option<u16>,

    #[serde(skip_serializing_if = "Option::is_none")]
    retain_acked_messages: Option<bool>,

    #[serde(skip_serializing_if = "Option::is_none")]
    message_retention_duration: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    labels: Option<HashMap<String, String>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    enable_message_ordering: Option<bool>,

    #[serde(skip_serializing_if = "Option::is_none")]
    expiration_policy: Option<BrokerSuscriptionExpirationPolicy>,

    #[serde(skip_serializing_if = "Option::is_none")]
    filter: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    dead_letter_policy: Option<BrokerSuscriptionDeadLetterPolicy>,

    #[serde(skip_serializing_if = "Option::is_none")]
    retry_policy: Option<BrokerSuscriptionRetryPolicy>,

    #[serde(skip_serializing_if = "Option::is_none")]
    detached: Option<bool>,

    #[serde(skip_serializing_if = "Option::is_none")]
    topic_message_retention_duration: Option<String>,
}

//
// Broker Errors
//
#[derive(Error, Debug)]
pub enum GCPPubsubError {
    #[error("Request error: {message}")]
    RequestError { message: String, status: u16 },

    #[error("Unknown error: {0}")]
    UnknownError(String),

    #[error("Client error: {0}")]
    RestClientError(#[from] reqwest::Error),
}

// Broker builder
struct Config {
    broker_url: String,
    prefetch_count: u16,
}

pub struct GCPPubSubBrokerBuilder {
    config: Config,
}

impl GCPPubSubBrokerBuilder {
    fn get_broker_configuration() -> Result<BrokerOptions, BrokerError> {
        if let Ok(config_file_path) = std::env::var("GCPPUBSUB_CONFIG") {
            let config_file = File::open(config_file_path).map_err(BrokerError::IoError)?;
            let reader = BufReader::new(config_file);
            serde_json::from_reader(reader).map_err(BrokerError::DeserializeError)
        } else {
            warn!(
                "No configuration file defined in environment variable \"GCPPUBSUB_CONFIG\". \
                 Using default values."
            );
            Ok(BrokerOptions::default())
        }
    }
}

#[async_trait]
impl BrokerBuilder for GCPPubSubBrokerBuilder {
    type Broker = GCPPubSubBroker;

    fn new(broker_url: &str) -> Self {
        Self {
            config: Config {
                broker_url: broker_url.into(),
                prefetch_count: 10,
            },
        }
    }

    fn prefetch_count(mut self, prefetch_count: u16) -> Self {
        self.config.prefetch_count = prefetch_count;
        self
    }

    fn declare_queue(self, _queue: &str) -> Self {
        self
    }

    fn heartbeat(self, _heartbeat: Option<u16>) -> Self {
        self
    }

    async fn build(&self, _connection_timeout: u32) -> Result<Self::Broker, BrokerError> {
        // Get topic subscription options
        let broker_configuration = Self::get_broker_configuration()?;
        let project = self.config.broker_url.split('/').collect::<Vec<&str>>();
        let project = project
            .last()
            .expect("Unable to get the GCP project from the Url");

        Ok(GCPPubSubBroker {
            base_url: self.config.broker_url.clone(),
            producer_options: broker_configuration.producer.unwrap_or_default(),
            consumer_options: broker_configuration.consumer.unwrap_or_default(),
            prefetch_count: Arc::new(AtomicU16::new(self.config.prefetch_count)),
            pending_tasks: Arc::new(AtomicU16::new(0)),
            gcp_project: project.to_string(),
        })
    }
}

pub struct GCPPubSubBroker {
    base_url: String,

    gcp_project: String,

    consumer_options: BrokerConsumerOptions,

    producer_options: BrokerProducerOptions,

    prefetch_count: Arc<AtomicU16>,

    pending_tasks: Arc<AtomicU16>,
}

#[async_trait]
impl Broker for GCPPubSubBroker {
    type Builder = GCPPubSubBrokerBuilder;
    type Delivery = (GCPChannel, GCPDelivery);
    type DeliveryError = BrokerError;
    type DeliveryStream = GCPConsumer;

    fn safe_url(&self) -> String {
        self.base_url.clone()
    }

    async fn consume<E: Fn(BrokerError) + Send + Sync + 'static>(
        &self,
        topic: &str,
        error_handler: Box<E>,
    ) -> Result<(String, Self::DeliveryStream), BrokerError> {
        let create_default_subscription = self.consumer_options.create_default_subscription;

        // Get the topic subscription... if there is one
        let topic_subscription = match &self.consumer_options.topics {
            Some(topic_subscriptions) => topic_subscriptions.get(topic),
            _ => None,
        };

        // Get the subscription depending on the options
        let subscription_options = match (topic_subscription, create_default_subscription) {
            (Some(subscription), _) => Some(subscription.to_owned()),
            (None, Some(true)) => Some(BrokerSubscription {
                name: format!("{}_default_subscription", topic),
                ..BrokerSubscription::default()
            }),
            _ => None,
        };

        if let Some(mut subscription) = subscription_options {
            // Set the topic related to the subscription
            subscription.topic = format!("projects/{}/topics/{}", self.gcp_project, topic);

            // Create unique consumer tag.
            let mut buffer = Uuid::encode_buffer();
            let uuid = Uuid::new_v4().to_hyphenated().encode_lower(&mut buffer);
            let consumer_tag = uuid.to_owned();

            // Create the channel
            let gcp_channel = GCPChannel::new(
                self.base_url.clone(),
                topic.to_string(),
                subscription.clone(),
            );

            // Susbcribe to the topic with the given subscription
            gcp_channel.subscribe_to_topic().await?;

            let consumer = GCPConsumer {
                pending_tasks: self.pending_tasks.clone(),
                prefetch_count: self.prefetch_count.clone(),
                polled_pop: None,
                channel: gcp_channel,
                error_handler,
            };

            Ok((consumer_tag, consumer))
        } else {
            Err(BrokerError::UnknownQueue(format!(
                "There is no subscription configuration for topic \"{}\".",
                topic
            )))
        }
    }

    async fn cancel(&self, _consumer_tag: &str) -> Result<(), BrokerError> {
        Ok(())
    }

    async fn ack(&self, delivery: &Self::Delivery) -> Result<(), BrokerError> {
        let (channel, delivered_message) = delivery;

        channel
            .acknowledge(delivered_message.acknowledge_id.clone())
            .await
    }

    async fn nack(&self, delivery: &Self::Delivery) -> Result<(), BrokerError> {
        let (channel, delivered_message) = delivery;

        // Send nack to pubsub
        channel
            .modify_ack_deadline(delivered_message.acknowledge_id.clone(), 0)
            .await
    }

    async fn retry(
        &self,
        delivery: &Self::Delivery,
        _eta: Option<DateTime<Utc>>,
    ) -> Result<(), BrokerError> {
        let (channel, _) = delivery;
        let mut message = delivery.try_deserialize_message()?;

        message.headers.retries = match message.headers.retries {
            Some(retry_number) => Some(retry_number + 1),
            None => Some(1),
        };

        channel.send_message(&message).await
    }

    async fn send(&self, message: &Message, topic: &str) -> Result<(), BrokerError> {
        // We use the default subscription because we do not need a subscription to send a message
        // to a topic.
        let channel = GCPChannel::new(
            self.base_url.clone(),
            topic.to_string(),
            BrokerSubscription::default(),
        );

        match channel.send_message(message).await {
            Ok(_) => Ok(()),
            Err(BrokerError::GCPPubsubError(GCPPubsubError::RequestError {
                status: 404,
                message: error_message,
            })) => {
                // If we have the producer configured to create the topic if it does not exist...
                if let Some(true) = self.producer_options.create_topic {
                    // Create the topic...
                    info!("Topic \"{}\" does not exist. Creating it...", topic);
                    channel.create_topic().await?;

                    // Resend the message
                    channel.send_message(message).await
                } else {
                    Err(GCPPubsubError::RequestError {
                        message: error_message,
                        status: 404,
                    }
                    .into())
                }
            }
            Err(e) => Err(e),
        }
    }

    async fn increase_prefetch_count(&self) -> Result<(), BrokerError> {
        self.prefetch_count.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn decrease_prefetch_count(&self) -> Result<(), BrokerError> {
        self.prefetch_count.fetch_sub(1, Ordering::SeqCst);
        Ok(())
    }

    async fn close(&self) -> Result<(), BrokerError> {
        // There is no socket so we don't close the connection
        Ok(())
    }

    async fn reconnect(&self, _connection_timeout: u32) -> Result<(), BrokerError> {
        // There is no active connection so we don't reconnect
        Ok(())
    }

    async fn on_message_processed(&self, delivery: &Self::Delivery) -> Result<(), BrokerError> {
        // If the task finished, means we can decrement the pending_task
        self.pending_tasks.fetch_sub(1, Ordering::SeqCst);

        // If the pending tasks are less than the prefecth, we wake up the stream to continue
        // processing messages
        if self.pending_tasks.load(Ordering::SeqCst) < self.prefetch_count.load(Ordering::SeqCst) {
            if let Some(waker) = &delivery.1.waker {
                waker.wake_by_ref();
            }
        }

        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct GCPChannel {
    connection: reqwest::Client,
    base_url: String,
    topic: String,
    subscription: BrokerSubscription,
}

impl GCPChannel {
    const ERROR_GCP_ERROR_MESSAGE_PARSE: &'static str =
        "There was an error parsing a GCP Error Response";

    fn new(base_url: String, topic: String, subscription: BrokerSubscription) -> Self {
        // Build reqwest client
        let mut default_headers = header::HeaderMap::new();
        default_headers.insert(
            header::CONTENT_TYPE,
            header::HeaderValue::from_static("application/json"),
        );

        let client = reqwest::ClientBuilder::new()
            .default_headers(default_headers)
            .build()
            .expect("There was an error building the REST client for GCP Pubsub.");

        GCPChannel {
            connection: client,
            base_url,
            topic,
            subscription,
        }
    }

    async fn pull_message(self) -> GCPConsumerOutput {
        // We have this loop in case the long polling request returns nothing. We just pull again
        let deserealized_msg = loop {
            let response = self
                .connection
                .post(format!(
                    "{}/subscriptions/{}:pull",
                    &self.base_url, &self.subscription.name
                ))
                .body(r#"{ "maxMessages": 1 }"#)
                .send()
                .await
                .map_err(GCPPubsubError::RestClientError)?;

            let response_body = response
                .text()
                .await
                .map_err(GCPPubsubError::RestClientError)?;

            let deserialized = serde_json::from_str::<PubsubPullResponse>(&response_body)
                .map_err(BrokerError::DeserializeError)?;

            if let Some(messages) = deserialized.received_messages {
                break messages;
            }
        };

        // This error should never happen...
        let received_message = deserealized_msg.get(0).ok_or_else(|| {
            GCPPubsubError::UnknownError("Invalid request body, received empty message".to_owned())
        })?;

        let data = base64::decode(&received_message.message.data).map_err(|e| {
            BrokerError::IoError(std::io::Error::new(ErrorKind::InvalidData, e.to_string()))
        })?;

        let delivery: protocol::Delivery =
            serde_json::from_slice(&data).map_err(BrokerError::DeserializeError)?;

        Ok(GCPDelivery {
            acknowledge_id: received_message.ack_id.clone(),
            suscription: self.subscription.name,
            waker: None,
            delivery,
        })
    }

    async fn send_message(&self, message: &Message) -> Result<(), BrokerError> {
        let message_payload = base64::encode(message.json_serialized()?);
        let formatted_message = json!({ "messages": [{ "data": message_payload }] }).to_string();

        let response = self
            .connection
            .post(format!("{}/topics/{}:publish", &self.base_url, &self.topic))
            .body(formatted_message)
            .send()
            .await
            .map_err(GCPPubsubError::RestClientError)?;

        let response_status = response.status().as_u16();
        if response_status == 404 {
            let error_message = match self.get_deserialized_error(response).await {
                Some(e) => format!(
                    "{}: {}\n{}",
                    MESSAGE_TOPIC_NOT_EXIST, &self.topic, e.message
                ),
                None => format!("{}: {}", MESSAGE_TOPIC_NOT_EXIST, &self.topic),
            };

            return Err((GCPPubsubError::RequestError {
                message: error_message,
                status: response_status,
            })
            .into());
        }

        Ok(())
    }

    async fn acknowledge(&self, acknowledge_id: String) -> Result<(), BrokerError> {
        self.connection
            .post(format!(
                "{}/subscriptions/{}:acknowledge",
                &self.base_url, &self.subscription.name
            ))
            .body(format!(r#"{{ "ackIds": [ "{}" ] }}"#, acknowledge_id))
            .send()
            .await
            .map_err(GCPPubsubError::RestClientError)?;

        Ok(())
    }

    async fn modify_ack_deadline(
        &self,
        acknowledge_id: String,
        ack_deadline_seconds: u16,
    ) -> Result<(), BrokerError> {
        self.connection
            .post(format!(
                "{}/subscriptions/{}:modifyAckDeadline",
                &self.base_url, &self.subscription.name
            ))
            .body(format!(
                r#"{{ "ackIds": [ "{}" ], "ackDeadlineSeconds": {} }}"#,
                acknowledge_id, ack_deadline_seconds
            ))
            .send()
            .await
            .map_err(GCPPubsubError::RestClientError)?;

        Ok(())
    }

    async fn create_topic(&self) -> Result<(), BrokerError> {
        self.connection
            .put(format!("{}/topics/{}", &self.base_url, &self.topic))
            .send()
            .await
            .map_err(GCPPubsubError::RestClientError)?;

        Ok(())
    }

    async fn subscribe_to_topic(&self) -> Result<(), BrokerError> {
        match serde_json::to_string(&self.subscription) {
            Ok(subscription_json) => {
                let response = self
                    .connection
                    .put(format!(
                        "{}/subscriptions/{}",
                        &self.base_url, &self.subscription.name
                    ))
                    .body(subscription_json)
                    .send()
                    .await
                    .map_err(GCPPubsubError::RestClientError)?;

                let response_status = response.status().as_u16();
                if response_status == 404 {
                    let error_message = match self.get_deserialized_error(response).await {
                        Some(e) => format!("{}: {}\n{}", MESSAGE_TOPIC_NOT_EXIST, &self.topic, e.message),
                        None => format!("{}: {}", MESSAGE_TOPIC_NOT_EXIST, &self.topic),
                    };

                    return Err((GCPPubsubError::RequestError {
                        message: error_message,
                        status: response_status
                    }).into());
                }

                Ok(())
            }
            Err(e) => {
                Err(GCPPubsubError::UnknownError(format!(
                    "There was an error parsing the subscription options for topic \"{}\". Please check the configuration.\n{}",
                    &self.topic,
                    e.to_string()
                ))
                .into())
            }
        }
    }

    async fn get_deserialized_error(&self, response: reqwest::Response) -> Option<GCPErrorDetail> {
        let response_body = response.text().await;

        match response_body {
            Ok(body) => {
                let deserialized_error = serde_json::from_str::<GCPErrorMessage>(&body);

                match deserialized_error {
                    Ok(error) => Some(error.error),
                    Err(e) => {
                        error!("{}: {}", Self::ERROR_GCP_ERROR_MESSAGE_PARSE, e.to_string());
                        None
                    }
                }
            }
            Err(e) => {
                error!("{}: {}", Self::ERROR_GCP_ERROR_MESSAGE_PARSE, e.to_string());
                None
            }
        }
    }
}

#[derive(Clone, Debug)]
pub struct GCPDelivery {
    acknowledge_id: String,
    suscription: String,
    waker: Option<Waker>,
    delivery: protocol::Delivery,
}

impl TryDeserializeMessage for (GCPChannel, GCPDelivery) {
    fn try_deserialize_message(&self) -> Result<Message, ProtocolError> {
        self.1.delivery.try_deserialize_message()
    }
}

type GCPConsumerOutput = Result<GCPDelivery, BrokerError>;
type GCPConsumerOutputFuture = Box<dyn Future<Output = Result<GCPDelivery, BrokerError>>>;

pub struct GCPConsumer {
    pending_tasks: Arc<AtomicU16>,
    prefetch_count: Arc<AtomicU16>,
    channel: GCPChannel,
    polled_pop: Option<std::pin::Pin<GCPConsumerOutputFuture>>,
    error_handler: Box<dyn Fn(BrokerError) + Send + Sync + 'static>,
}

impl Stream for GCPConsumer {
    type Item = Result<(GCPChannel, GCPDelivery), BrokerError>;

    fn poll_next(mut self: std::pin::Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        // If we have more pending tasks than the prefetch count, we leave them pending until we
        // can process them...
        if self.pending_tasks.load(Ordering::SeqCst) >= self.prefetch_count.load(Ordering::SeqCst)
            && self.prefetch_count.load(Ordering::SeqCst) > 0
        {
            // If acks_late is true, this Pending is waken up by the ack function when a pending
            // task is terminated!
            return Poll::Pending;
        }

        // If the polled_pop is None, means we have to pull a message from the queue, otherwise we
        // have a Future that can be Resolved or Pending
        let mut polled_message = if self.polled_pop.is_none() {
            Box::pin(self.channel.clone().pull_message())
        } else {
            // It is safe to unwrap here since we have the is_none in the if branch
            self.polled_pop.take().unwrap()
        };

        // To execute the pull_message function, since it is an async Task we have to Poll it.
        //
        // If it is Ready:
        //  - If the task succeed, we add one to the pending_task and return the trask
        //  - Otherwise we handle the error, notify the executor the Stream is ready to run again,
        //    and return the Pending state
        //
        // If it is NOT Ready: Put the Future inside the Option again and return the Pending state
        if let Poll::Ready(item) = Future::poll(polled_message.as_mut(), cx) {
            match item {
                Ok(mut item) => {
                    self.pending_tasks.fetch_add(1, Ordering::SeqCst);
                    item.waker = Some(cx.waker().clone());
                    Poll::Ready(Some(Ok((self.channel.clone(), item))))
                }
                Err(err) => {
                    (self.error_handler)(err);
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
            }
        } else {
            self.polled_pop = Some(polled_message);
            // The poll function will tell us when to wake up
            Poll::Pending
        }
    }
}
