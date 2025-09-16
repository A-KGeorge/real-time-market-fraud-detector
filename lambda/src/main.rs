use anyhow::{anyhow, Result};
use lambda_runtime::{run, service_fn, Error, LambdaEvent};
use log::{error, info, warn};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::env;
use tokio::time::{timeout, Duration};
use uuid::Uuid;
use futures::future;

mod ecs_client;
mod sqs_handler;

use ecs_client::EcsClient;
use sqs_handler::{SqsMessage, SqsRecord};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LambdaConfig {
    pub ecs_cluster_name: String,
    pub ecs_task_definition: String,
    pub ecs_subnet_ids: Vec<String>,
    pub ecs_security_group_ids: Vec<String>,
    pub max_timeout_seconds: u64,
    pub max_concurrent_tasks: usize,
    pub aws_region: String,
}

impl LambdaConfig {
    pub fn from_env() -> Result<Self> {
        let subnet_ids = env::var("ECS_SUBNET_IDS")
            .unwrap_or_else(|_| "subnet-12345,subnet-67890".to_string())
            .split(',')
            .map(|s| s.trim().to_string())
            .collect();

        let security_group_ids = env::var("ECS_SECURITY_GROUP_IDS")
            .unwrap_or_else(|_| "sg-12345".to_string())
            .split(',')
            .map(|s| s.trim().to_string())
            .collect();

        Ok(Self {
            ecs_cluster_name: env::var("ECS_CLUSTER_NAME")
                .unwrap_or_else(|_| "market-surveillance-cluster".to_string()),
            ecs_task_definition: env::var("ECS_TASK_DEFINITION")
                .unwrap_or_else(|_| "python-inference-service".to_string()),
            ecs_subnet_ids: subnet_ids,
            ecs_security_group_ids: security_group_ids,
            max_timeout_seconds: env::var("MAX_TIMEOUT_SECONDS")
                .unwrap_or_else(|_| "30".to_string())
                .parse()
                .map_err(|e| anyhow!("Invalid MAX_TIMEOUT_SECONDS: {}", e))?,
            max_concurrent_tasks: env::var("MAX_CONCURRENT_TASKS")
                .unwrap_or_else(|_| "10".to_string())
                .parse()
                .map_err(|e| anyhow!("Invalid MAX_CONCURRENT_TASKS: {}", e))?,
            aws_region: env::var("AWS_REGION")
                .unwrap_or_else(|_| "us-east-1".to_string()),
        })
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct LambdaResponse {
    pub processed_messages: usize,
    pub successful_tasks: usize,
    pub failed_tasks: usize,
    pub task_arns: Vec<String>,
    pub correlation_id: String,
    pub processing_time_ms: u64,
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    env_logger::init();
    info!("Starting Market Lambda Processor");

    let config = LambdaConfig::from_env()
        .map_err(|e| Error::from(format!("Configuration error: {}", e)))?;

    info!("Lambda configuration loaded: {:?}", config);

    run(service_fn(|event: LambdaEvent<Value>| {
        lambda_handler(event, config.clone())
    }))
    .await
}

async fn lambda_handler(
    event: LambdaEvent<Value>,
    config: LambdaConfig,
) -> Result<LambdaResponse, Error> {
    let correlation_id = Uuid::new_v4().to_string();
    let start_time = std::time::Instant::now();

    info!(
        "Processing Lambda event with correlation_id: {}",
        correlation_id
    );

    // Parse SQS event
    let sqs_event: SqsMessage = serde_json::from_value(event.payload)
        .map_err(|e| Error::from(format!("Failed to parse SQS event: {}", e)))?;

    let total_records = sqs_event.records.len();
    info!("Received {} SQS records to process", total_records);

    if total_records == 0 {
        warn!("No records to process in SQS event");
        return Ok(LambdaResponse {
            processed_messages: 0,
            successful_tasks: 0,
            failed_tasks: 0,
            task_arns: vec![],
            correlation_id,
            processing_time_ms: start_time.elapsed().as_millis() as u64,
        });
    }

    // Initialize ECS client
    let ecs_client = EcsClient::new(&config.aws_region).await?;

    // Process records with timeout and concurrency control
    let processing_timeout = Duration::from_secs(config.max_timeout_seconds);
    let semaphore = std::sync::Arc::new(tokio::sync::Semaphore::new(config.max_concurrent_tasks));

    let mut successful_count = 0;
    let mut failed_count = 0;
    let mut task_arns = Vec::new();

    // Process records in batches with concurrency control
    let mut handles = Vec::new();

    for (index, record) in sqs_event.records.into_iter().enumerate() {
        let client = ecs_client.clone();
        let config = config.clone();
        let permit = semaphore.clone().acquire_owned().await
            .map_err(|e| Error::from(format!("Failed to acquire semaphore: {}", e)))?;
        let record_correlation_id = format!("{}-{}", correlation_id, index);

        let handle = tokio::spawn(async move {
            let _permit = permit; // Keep permit alive
            process_single_record(client, config, record, record_correlation_id).await
        });

        handles.push(handle);
    }

    // Wait for all processing to complete with timeout
    let results = match timeout(processing_timeout, future::join_all(handles)).await {
        Ok(results) => results,
        Err(_) => {
            error!("Processing timeout exceeded: {} seconds", config.max_timeout_seconds);
            return Err(Error::from("Processing timeout exceeded"));
        }
    };

    // Count results and collect task ARNs
    for result in results {
        match result {
            Ok(Ok(task_arn)) => {
                successful_count += 1;
                task_arns.push(task_arn);
            }
            Ok(Err(e)) => {
                error!("Record processing failed: {}", e);
                failed_count += 1;
            }
            Err(e) => {
                error!("Task join error: {}", e);
                failed_count += 1;
            }
        }
    }

    let processing_time = start_time.elapsed().as_millis() as u64;

    info!(
        "Processing completed - Total: {}, Successful: {}, Failed: {}, Tasks: {:?}, Time: {}ms",
        total_records, successful_count, failed_count, task_arns, processing_time
    );

    Ok(LambdaResponse {
        processed_messages: total_records,
        successful_tasks: successful_count,
        failed_tasks: failed_count,
        task_arns,
        correlation_id,
        processing_time_ms: processing_time,
    })
}

async fn process_single_record(
    ecs_client: EcsClient,
    config: LambdaConfig,
    record: SqsRecord,
    correlation_id: String,
) -> Result<String> {
    info!("Processing record with correlation_id: {}", correlation_id);

    // Parse the SQS message body
    let market_data: serde_json::Value = serde_json::from_str(&record.body)
        .map_err(|e| anyhow!("Failed to parse message body: {}", e))?;

    info!("Parsed market data for symbol: {}", 
          market_data.get("symbol").unwrap_or(&Value::String("unknown".to_string())));

    // Run ECS task with market data as environment override
    match ecs_client.run_inference_task(&config, market_data, correlation_id.clone()).await {
        Ok(task_arn) => {
            info!(
                "Successfully started ECS task {} for correlation_id: {}",
                task_arn, correlation_id
            );
            Ok(task_arn)
        }
        Err(e) => {
            error!(
                "Failed to start ECS task for correlation_id {}: {}",
                correlation_id, e
            );
            Err(e)
        }
    }
}