use crate::LambdaConfig;
use anyhow::{anyhow, Result};
use aws_config::BehaviorVersion;
use aws_sdk_ecs::{
    types::{
        AssignPublicIp, AwsVpcConfiguration, ContainerOverride, KeyValuePair, LaunchType,
        NetworkConfiguration, TaskOverride,
    },
    Client,
};
use log::{error, info};
use serde_json::Value;

#[derive(Debug, Clone)]
pub struct EcsClient {
    client: Client,
}

impl EcsClient {
    pub async fn new(region: &str) -> Result<Self> {
        let region_str = region.to_string();
        let config = aws_config::defaults(BehaviorVersion::latest())
            .region(aws_config::Region::new(region_str))
            .load()
            .await;

        let client = Client::new(&config);

        info!("ECS client initialized for region: {}", region);

        Ok(Self { client })
    }

    pub async fn run_inference_task(
        &self,
        config: &LambdaConfig,
        market_data: Value,
        correlation_id: String,
    ) -> Result<String> {
        info!("Starting ECS task for correlation_id: {}", correlation_id);

        // Prepare environment variables for the container
        let mut environment_overrides = vec![
            KeyValuePair::builder()
                .name("CORRELATION_ID")
                .value(&correlation_id)
                .build(),
            KeyValuePair::builder()
                .name("MARKET_DATA")
                .value(serde_json::to_string(&market_data)?)
                .build(),
            KeyValuePair::builder()
                .name("PROCESSING_MODE")
                .value("LAMBDA_TRIGGERED")
                .build(),
        ];

        // Add symbol if available
        if let Some(symbol) = market_data.get("symbol").and_then(|v| v.as_str()) {
            environment_overrides.push(
                KeyValuePair::builder()
                    .name("TARGET_SYMBOL")
                    .value(symbol)
                    .build(),
            );
        }

        // Create container override
        let container_override = ContainerOverride::builder()
            .name("python-inference") // Should match your task definition
            .set_environment(Some(environment_overrides))
            .build();

        // Create task override
        let task_override = TaskOverride::builder()
            .container_overrides(container_override)
            .build();

        // Create network configuration for Fargate
        let vpc_config = AwsVpcConfiguration::builder()
            .set_subnets(Some(config.ecs_subnet_ids.clone()))
            .set_security_groups(Some(config.ecs_security_group_ids.clone()))
            .assign_public_ip(AssignPublicIp::Enabled) // Needed for Fargate with public subnets
            .build()
            .map_err(|e| anyhow::anyhow!("Failed to build VPC configuration: {}", e))?;

        let network_config = NetworkConfiguration::builder()
            .awsvpc_configuration(vpc_config)
            .build();

        // Run the task
        let run_task_response = self
            .client
            .run_task()
            .cluster(&config.ecs_cluster_name)
            .task_definition(&config.ecs_task_definition)
            .launch_type(LaunchType::Fargate)
            .network_configuration(network_config)
            .overrides(task_override)
            .send()
            .await
            .map_err(|e| anyhow!("Failed to run ECS task: {}", e))?;

        // Get the task ARN
        if let Some(tasks) = run_task_response.tasks {
            if let Some(task) = tasks.first() {
                if let Some(task_arn) = &task.task_arn {
                    info!("Successfully started ECS task: {}", task_arn);
                    return Ok(task_arn.clone());
                }
            }
        }

        // Check for failures
        if let Some(failures) = run_task_response.failures {
            let failure_reasons: Vec<String> = failures
                .iter()
                .map(|f| {
                    format!(
                        "{}:{}",
                        f.arn().unwrap_or("unknown"),
                        f.reason().unwrap_or("no reason")
                    )
                })
                .collect();
            error!("ECS task failures: {:?}", failure_reasons);
            return Err(anyhow!("ECS task failed to start: {:?}", failure_reasons));
        }

        Err(anyhow!("No task ARN returned from ECS runTask"))
    }

    pub async fn check_task_status(&self, cluster: &str, task_arn: &str) -> Result<String> {
        let describe_response = self
            .client
            .describe_tasks()
            .cluster(cluster)
            .tasks(task_arn)
            .send()
            .await
            .map_err(|e| anyhow!("Failed to describe ECS task: {}", e))?;

        if let Some(tasks) = describe_response.tasks {
            if let Some(task) = tasks.first() {
                if let Some(last_status) = &task.last_status {
                    return Ok(last_status.clone());
                }
            }
        }

        Ok("UNKNOWN".to_string())
    }

    pub async fn run_batch_inference_task(
        &self,
        config: &LambdaConfig,
        batch_market_data: Vec<Value>,
        correlation_id: String,
    ) -> Result<String> {
        info!("Starting batch ECS task for correlation_id: {} with {} records", correlation_id, batch_market_data.len());

        // Prepare environment variables for the container
        let mut environment_overrides = vec![
            KeyValuePair::builder()
                .name("CORRELATION_ID")
                .value(&correlation_id)
                .build(),
            KeyValuePair::builder()
                .name("BATCH_MARKET_DATA")
                .value(serde_json::to_string(&batch_market_data)?)
                .build(),
            KeyValuePair::builder()
                .name("PROCESSING_MODE")
                .value("LAMBDA_TRIGGERED")
                .build(),
        ];

        // Add batch size info
        environment_overrides.push(
            KeyValuePair::builder()
                .name("BATCH_SIZE")
                .value(batch_market_data.len().to_string())
                .build(),
        );

        // Create container override
        let container_override = ContainerOverride::builder()
            .name("python-inference") // Should match your task definition
            .set_environment(Some(environment_overrides))
            .build();

        // Create task override
        let task_override = TaskOverride::builder()
            .container_overrides(container_override)
            .build();

        // Create network configuration for Fargate
        let vpc_config = AwsVpcConfiguration::builder()
            .set_subnets(Some(config.ecs_subnet_ids.clone()))
            .set_security_groups(Some(config.ecs_security_group_ids.clone()))
            .assign_public_ip(AssignPublicIp::Enabled) // Needed for Fargate with public subnets
            .build()
            .map_err(|e| anyhow::anyhow!("Failed to build VPC configuration: {}", e))?;

        let network_config = NetworkConfiguration::builder()
            .awsvpc_configuration(vpc_config)
            .build();

        // Run the task
        let run_task_response = self
            .client
            .run_task()
            .cluster(&config.ecs_cluster_name)
            .task_definition(&config.ecs_task_definition)
            .launch_type(LaunchType::Fargate)
            .network_configuration(network_config)
            .overrides(task_override)
            .send()
            .await
            .map_err(|e| anyhow!("Failed to run batch ECS task: {}", e))?;

        // Get the task ARN
        if let Some(tasks) = run_task_response.tasks {
            if let Some(task) = tasks.first() {
                if let Some(task_arn) = &task.task_arn {
                    info!("Successfully started batch ECS task: {}", task_arn);
                    return Ok(task_arn.clone());
                }
            }
        }

        // Check for failures
        if let Some(failures) = run_task_response.failures {
            let failure_reasons: Vec<String> = failures
                .iter()
                .map(|f| {
                    format!(
                        "{}:{}",
                        f.arn().unwrap_or("unknown"),
                        f.reason().unwrap_or("no reason")
                    )
                })
                .collect();
            error!("Batch ECS task failures: {:?}", failure_reasons);
            return Err(anyhow!("Batch ECS task failed to start: {:?}", failure_reasons));
        }

        Err(anyhow!("No task ARN returned from batch ECS runTask"))
    }
}
