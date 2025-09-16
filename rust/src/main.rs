use anyhow::Result;
use log::{error, info, warn};
use std::time::Duration;
use tokio::signal;
use tokio::time;

mod config;
mod models;
mod sqs_client;
mod yahoo_client;

use config::Config;
use sqs_client::SqsClient;
use yahoo_client::YahooFinanceClient;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    env_logger::init();

    info!("Starting Market Ingestion Service");

    // Load configuration
    let config = Config::from_env()?;
    info!("Configuration loaded: {:?}", config);

    // Initialize clients
    let yahoo_client = YahooFinanceClient::new();
    let sqs_client = SqsClient::new(&config.aws_region, &config.sqs_queue_url).await?;

    info!("Clients initialized successfully");

    // Set up graceful shutdown
    let shutdown_signal = async {
        signal::ctrl_c()
            .await
            .expect("Failed to install CTRL+C signal handler");
        info!("Received shutdown signal, gracefully shutting down...");
    };

    // Main ingestion loop with graceful shutdown
    let ingestion_loop = async {
        loop {
            let start_time = std::time::Instant::now();

            match run_ingestion_cycle(&config, &yahoo_client, &sqs_client).await {
                Ok(processed) => {
                    let duration = start_time.elapsed();
                    info!(
                        "Ingestion cycle completed: {} symbols processed in {:?}",
                        processed, duration
                    );
                }
                Err(e) => {
                    error!("Ingestion cycle failed: {}", e);
                }
            }

            // Wait before next cycle (configurable interval)
            tokio::select! {
                _ = time::sleep(Duration::from_secs(config.ingestion_interval_seconds)) => {},
                _ = signal::ctrl_c() => {
                    info!("Received shutdown signal during sleep, exiting...");
                    break;
                }
            }
        }
    };

    // Run either the ingestion loop or wait for shutdown signal
    tokio::select! {
        _ = ingestion_loop => {},
        _ = shutdown_signal => {},
    }

    info!("Market Ingestion Service stopped gracefully");
    Ok(())
}

async fn run_ingestion_cycle(
    config: &Config,
    yahoo_client: &YahooFinanceClient,
    sqs_client: &SqsClient,
) -> Result<usize> {
    let mut processed_count = 0;

    for symbol in &config.symbols {
        match process_symbol(symbol, yahoo_client, sqs_client, config.test_mode).await {
            Ok(_) => {
                processed_count += 1;
                info!("Successfully processed symbol: {}", symbol);
            }
            Err(e) => {
                warn!("Failed to process symbol {}: {}", symbol, e);
                // Continue with other symbols even if one fails
            }
        }

        // Small delay between symbol requests to be respectful to Yahoo Finance
        time::sleep(Duration::from_millis(100)).await;
    }

    Ok(processed_count)
}

async fn process_symbol(
    symbol: &str,
    yahoo_client: &YahooFinanceClient,
    sqs_client: &SqsClient,
    test_mode: bool,
) -> Result<()> {
    // Fetch market data from Yahoo Finance
    let market_data = yahoo_client
        .fetch_latest_data_with_mode(symbol, test_mode)
        .await?;

    // Send to SQS for Python service processing
    sqs_client.send_market_data(symbol, &market_data).await?;

    Ok(())
}
