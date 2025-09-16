use anyhow::{anyhow, Result};
use chrono::Utc;
use governor::{Quota, RateLimiter};
use log::info;
use rand::Rng;
use reqwest::Client;
use std::num::NonZeroU32;
use std::time::Duration;
use yahoo_finance_api as yahoo;

use crate::models::MarketData;

pub struct YahooFinanceClient {
    #[allow(dead_code)]
    client: Client,
    #[allow(dead_code)]
    base_url: String,
    rate_limiter: RateLimiter<
        governor::state::direct::NotKeyed,
        governor::state::InMemoryState,
        governor::clock::DefaultClock,
    >,
    yahoo_provider: yahoo::YahooConnector,
}

impl YahooFinanceClient {
    pub fn new() -> Self {
        let client = Client::builder()
            .timeout(Duration::from_secs(30))
            .user_agent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
            .default_headers({
                let mut headers = reqwest::header::HeaderMap::new();
                headers.insert("Accept", "application/json".parse().unwrap());
                headers.insert("Accept-Language", "en-US,en;q=0.9".parse().unwrap());
                headers.insert("Cache-Control", "no-cache".parse().unwrap());
                headers.insert("Pragma", "no-cache".parse().unwrap());
                headers
            })
            .build()
            .expect("Failed to create HTTP client");

        // Rate limiter: 5 requests per second to be more conservative
        let quota = Quota::per_second(NonZeroU32::new(5).unwrap());
        let rate_limiter = RateLimiter::direct(quota);

        let yahoo_provider = yahoo::YahooConnector::new().unwrap();

        Self {
            client,
            base_url: "https://query1.finance.yahoo.com".to_string(),
            rate_limiter,
            yahoo_provider,
        }
    }

    #[allow(dead_code)]
    pub async fn fetch_latest_data(&self, symbol: &str) -> Result<MarketData> {
        self.fetch_latest_data_with_mode(symbol, false).await
    }

    pub async fn fetch_latest_data_with_mode(
        &self,
        symbol: &str,
        test_mode: bool,
    ) -> Result<MarketData> {
        if test_mode {
            return self.generate_mock_data(symbol);
        }

        // Rate limiting
        self.rate_limiter.until_ready().await;

        info!("Fetching real data for symbol: {}", symbol);

        // Use the yahoo_finance_api crate which handles the API properly
        let response = self
            .yahoo_provider
            .get_latest_quotes(symbol, "1d")
            .await
            .map_err(|e| anyhow!("Failed to fetch data for {}: {}", symbol, e))?;

        let quotes = response
            .quotes()
            .map_err(|e| anyhow!("Failed to parse quotes for {}: {}", symbol, e))?;

        if quotes.is_empty() {
            return Err(anyhow!("No quotes returned for symbol: {}", symbol));
        }

        // Get the latest quote
        let latest_quote = &quotes[quotes.len() - 1];

        let market_data = MarketData {
            symbol: symbol.to_string(),
            timestamp: Utc::now(),
            open: latest_quote.open,
            high: latest_quote.high,
            low: latest_quote.low,
            close: latest_quote.close,
            volume: latest_quote.volume as i64,
            previous_close: if quotes.len() > 1 {
                Some(quotes[quotes.len() - 2].close)
            } else {
                Some(latest_quote.close * 0.99) // Approximate if no previous data
            },
        };

        info!(
            "Successfully fetched real Yahoo Finance data for {}: price=${:.2}",
            symbol, market_data.close
        );

        Ok(market_data)
    }

    #[allow(dead_code)]
    pub async fn fetch_batch_data(&self, symbols: &[String]) -> Vec<Result<MarketData>> {
        let mut results = Vec::new();

        // Process symbols in batches to avoid overwhelming the API
        for symbol in symbols {
            let result = self.fetch_latest_data(symbol).await;
            results.push(result);

            // Small delay between requests
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        results
    }

    fn generate_mock_data(&self, symbol: &str) -> Result<MarketData> {
        let mut rng = rand::thread_rng();

        // Generate realistic stock prices
        let base_price = match symbol {
            "AAPL" => 150.0,
            "GOOGL" => 140.0,
            "MSFT" => 300.0,
            "AMZN" => 130.0,
            "TSLA" => 200.0,
            "NVDA" => 400.0,
            "META" => 300.0,
            "NFLX" => 450.0,
            "AMD" => 100.0,
            "INTC" => 50.0,
            _ => 100.0, // Default for other symbols
        };

        // Add some random variation (+/- 5%)
        let variation = rng.gen_range(-0.05..0.05);
        let current_price = base_price * (1.0 + variation);

        let open = current_price * rng.gen_range(0.98..1.02);
        let high = f64::max(current_price, open) * rng.gen_range(1.0..1.03);
        let low = f64::min(current_price, open) * rng.gen_range(0.97..1.0);
        let volume = rng.gen_range(1_000_000..10_000_000);

        Ok(MarketData {
            symbol: symbol.to_string(),
            timestamp: Utc::now(),
            open,
            high,
            low,
            close: current_price,
            volume,
            previous_close: Some(current_price * rng.gen_range(0.98..1.02)),
        })
    }
}
