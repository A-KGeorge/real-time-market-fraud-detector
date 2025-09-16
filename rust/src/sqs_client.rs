use anyhow::{anyhow, Result};
use aws_config::BehaviorVersion;
use aws_sdk_sqs::{types::MessageAttributeValue, Client};
use chrono::Utc;
use log::{debug, info};
use uuid::Uuid;

use crate::models::{MarketData, SqsMarketMessage};

pub struct SqsClient {
    client: Client,
    queue_url: String,
}

impl SqsClient {
    pub async fn new(region: &str, queue_url: &str) -> Result<Self> {
        let region_str = region.to_string();
        let config = aws_config::defaults(BehaviorVersion::latest())
            .region(aws_config::Region::new(region_str))
            .load()
            .await;

        let client = Client::new(&config);

        info!("SQS client initialized for region: {}", region);
        debug!("Queue URL: {}", queue_url);

        Ok(Self {
            client,
            queue_url: queue_url.to_string(),
        })
    }

    pub async fn send_market_data(&self, symbol: &str, market_data: &MarketData) -> Result<()> {
        let message = SqsMarketMessage {
            message_type: "MARKET_DATA".to_string(), // Lambda expects uppercase
            symbol: symbol.to_string(),
            data: market_data.clone(),
            source: "yahoo_finance".to_string(), // Match Lambda expectations
            ingestion_timestamp: Utc::now(),
            batch_id: Uuid::new_v4().to_string(),
        };

        let message_body = serde_json::to_string(&message)
            .map_err(|e| anyhow!("Failed to serialize message: {}", e))?;

        // Create unique message deduplication ID for FIFO queue
        let deduplication_id = format!(
            "{}-{}-{}",
            symbol.to_uppercase(),
            message.ingestion_timestamp.timestamp(),
            &message.batch_id
        );

        // Message group ID for FIFO ordering - group by symbol
        let message_group_id = format!("market-data-{}", symbol.to_uppercase());

        debug!("Sending message to SQS for symbol: {}", symbol);

        let result = self
            .client
            .send_message()
            .queue_url(&self.queue_url)
            .message_body(&message_body)
            .message_deduplication_id(&deduplication_id)
            .message_group_id(&message_group_id)
            .message_attributes(
                "Symbol",
                MessageAttributeValue::builder()
                    .string_value(symbol)
                    .data_type("String")
                    .build()
                    .map_err(|e| anyhow!("Failed to build message attribute: {}", e))?,
            )
            .message_attributes(
                "Source",
                MessageAttributeValue::builder()
                    .string_value("yahoo_finance")
                    .data_type("String")
                    .build()
                    .map_err(|e| anyhow!("Failed to build message attribute: {}", e))?,
            )
            .message_attributes(
                "MessageType",
                MessageAttributeValue::builder()
                    .string_value("MARKET_DATA")
                    .data_type("String")
                    .build()
                    .map_err(|e| anyhow!("Failed to build message attribute: {}", e))?,
            )
            .message_attributes(
                "BatchId",
                MessageAttributeValue::builder()
                    .string_value(&message.batch_id)
                    .data_type("String")
                    .build()
                    .map_err(|e| anyhow!("Failed to build message attribute: {}", e))?,
            )
            .message_attributes(
                "Timestamp",
                MessageAttributeValue::builder()
                    .string_value(&message.ingestion_timestamp.to_rfc3339())
                    .data_type("String")
                    .build()
                    .map_err(|e| anyhow!("Failed to build message attribute: {}", e))?,
            )
            .send()
            .await
            .map_err(|e| anyhow!("Failed to send message to SQS: {}", e))?;

        info!(
            "Successfully sent message to SQS for symbol: {} (MessageId: {:?})",
            symbol,
            result.message_id()
        );

        Ok(())
    }

    #[allow(dead_code)]
    pub async fn send_batch_data(
        &self,
        data: Vec<(String, MarketData)>,
    ) -> Result<Vec<Result<()>>> {
        let mut results = Vec::new();

        for (symbol, market_data) in data {
            let result = self.send_market_data(&symbol, &market_data).await;
            results.push(result);
        }

        Ok(results)
    }

    #[allow(dead_code)]
    pub async fn health_check(&self) -> Result<()> {
        // Try to get queue attributes as a health check
        let _attributes = self
            .client
            .get_queue_attributes()
            .queue_url(&self.queue_url)
            .send()
            .await
            .map_err(|e| anyhow!("SQS health check failed: {}", e))?;

        info!("SQS health check passed");
        Ok(())
    }
}
