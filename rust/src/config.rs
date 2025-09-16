use anyhow::{anyhow, Result};
use std::env;

#[derive(Debug, Clone)]
pub struct Config {
    pub aws_region: String,
    pub sqs_queue_url: String,
    pub symbols: Vec<String>,
    pub ingestion_interval_seconds: u64,
    pub yahoo_finance_base_url: String,
    pub test_mode: bool,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        // Load .env file if it exists
        let _ = dotenvy::dotenv();

        let aws_region = env::var("AWS_REGION").unwrap_or_else(|_| "us-east-1".to_string());

        let sqs_queue_url = env::var("SQS_QUEUE_URL")
            .map_err(|_| anyhow!("SQS_QUEUE_URL environment variable is required"))?;

        // Default to the same 10 symbols used in the Python service, plus 19 more for 29 total
        let symbols_str = env::var("MARKET_DATA_SYMBOLS")
            .unwrap_or_else(|_| "AAPL,GOOGL,MSFT,AMZN,TSLA,NVDA,META,NFLX,AMD,INTC,BABA,JPM,JNJ,V,PG,UNH,HD,MA,PYPL,DIS,VZ,ADBE,CRM,CMCSA,PFE,KO,PEP,ABT,TMO".to_string());

        let symbols: Vec<String> = symbols_str
            .split(',')
            .map(|s| s.trim().to_uppercase())
            .collect();

        if symbols.is_empty() {
            return Err(anyhow!("No symbols configured"));
        }

        let ingestion_interval_seconds = env::var("INGESTION_INTERVAL_SECONDS")
            .unwrap_or_else(|_| "60".to_string())
            .parse::<u64>()
            .map_err(|_| anyhow!("Invalid INGESTION_INTERVAL_SECONDS"))?;

        let yahoo_finance_base_url = env::var("YAHOO_FINANCE_BASE_URL")
            .unwrap_or_else(|_| "https://query1.finance.yahoo.com".to_string());

        let test_mode = env::var("TEST_MODE")
            .unwrap_or_else(|_| "false".to_string())
            .to_lowercase()
            == "true";

        Ok(Config {
            aws_region,
            sqs_queue_url,
            symbols,
            ingestion_interval_seconds,
            yahoo_finance_base_url,
            test_mode,
        })
    }
}
