use anyhow::Result;
use log::{error, info, warn};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
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

    // Create atomic counter for round-robin symbol selection
    let symbol_index = Arc::new(AtomicUsize::new(0));

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

            match run_ingestion_cycle(&config, &yahoo_client, &sqs_client, &symbol_index).await {
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
    symbol_index: &Arc<AtomicUsize>,
) -> Result<usize> {
    // For frequent updates, process one symbol per cycle to distribute load evenly
    // This ensures all symbols get updated over time while maintaining high frequency
    let current_index = symbol_index.fetch_add(1, Ordering::SeqCst) % config.symbols.len();
    let symbol_to_process = &config.symbols[current_index];

    info!(
        "Processing symbol {} of {}: {}",
        current_index + 1,
        config.symbols.len(),
        symbol_to_process
    );

    match process_symbol(
        symbol_to_process,
        yahoo_client,
        sqs_client,
        config.test_mode,
    )
    .await
    {
        Ok(_) => {
            info!("Successfully processed symbol: {}", symbol_to_process);
            Ok(1)
        }
        Err(e) => {
            warn!("Failed to process symbol {}: {}", symbol_to_process, e);
            Ok(0) // Don't fail the entire cycle for one symbol
        }
    }
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
