use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketData {
    pub symbol: String,
    pub timestamp: DateTime<Utc>,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: i64,
    pub previous_close: Option<f64>,
}

// Yahoo Quote API structures
#[derive(Debug, Deserialize)]
pub struct YahooQuoteResponse {
    #[serde(rename = "quoteResponse")]
    #[allow(dead_code)]
    pub quote_response: QuoteResponse,
}

#[derive(Debug, Deserialize)]
pub struct QuoteResponse {
    #[allow(dead_code)]
    pub result: Vec<QuoteResult>,
    #[allow(dead_code)]
    pub error: Option<String>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct QuoteResult {
    pub symbol: String,
    #[serde(rename = "regularMarketPrice")]
    pub regular_market_price: Option<f64>,
    #[serde(rename = "regularMarketOpen")]
    pub regular_market_open: Option<f64>,
    #[serde(rename = "regularMarketDayHigh")]
    pub regular_market_day_high: Option<f64>,
    #[serde(rename = "regularMarketDayLow")]
    pub regular_market_day_low: Option<f64>,
    #[serde(rename = "regularMarketVolume")]
    pub regular_market_volume: Option<i64>,
    #[serde(rename = "regularMarketPreviousClose")]
    pub regular_market_previous_close: Option<f64>,
    #[serde(rename = "regularMarketTime")]
    pub regular_market_time: Option<i64>,
}

// Yahoo Chart API structures (alternative)
#[derive(Debug, Deserialize)]
pub struct YahooChartResponse {
    #[allow(dead_code)]
    pub chart: ChartData,
}

#[derive(Debug, Deserialize)]
pub struct ChartData {
    #[allow(dead_code)]
    pub result: Vec<ChartResult>,
    #[allow(dead_code)]
    pub error: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct ChartResult {
    #[allow(dead_code)]
    pub meta: ChartMeta,
    #[allow(dead_code)]
    pub indicators: ChartIndicators,
}

#[derive(Debug, Deserialize)]
pub struct ChartMeta {
    #[allow(dead_code)]
    pub symbol: String,
    #[serde(rename = "regularMarketPrice")]
    #[allow(dead_code)]
    pub regular_market_price: Option<f64>,
    #[serde(rename = "previousClose")]
    #[allow(dead_code)]
    pub previous_close: Option<f64>,
}

#[derive(Debug, Deserialize)]
pub struct ChartIndicators {
    #[allow(dead_code)]
    pub quote: Vec<QuoteData>,
}

#[derive(Debug, Deserialize)]
pub struct QuoteData {
    #[allow(dead_code)]
    pub open: Vec<Option<f64>>,
    #[allow(dead_code)]
    pub high: Vec<Option<f64>>,
    #[allow(dead_code)]
    pub low: Vec<Option<f64>>,
    #[allow(dead_code)]
    pub close: Vec<Option<f64>>,
    #[allow(dead_code)]
    pub volume: Vec<Option<i64>>,
}

#[derive(Debug, Serialize)]
pub struct SqsMarketMessage {
    pub message_type: String,
    pub symbol: String,
    pub data: MarketData,
    pub source: String,
    pub ingestion_timestamp: DateTime<Utc>,
    pub batch_id: String,
}

impl From<QuoteResult> for MarketData {
    fn from(quote: QuoteResult) -> Self {
        let timestamp = if let Some(market_time) = quote.regular_market_time {
            DateTime::from_timestamp(market_time, 0).unwrap_or_else(Utc::now)
        } else {
            Utc::now()
        };

        MarketData {
            symbol: quote.symbol.clone(),
            timestamp,
            open: quote.regular_market_open.unwrap_or(0.0),
            high: quote.regular_market_day_high.unwrap_or(0.0),
            low: quote.regular_market_day_low.unwrap_or(0.0),
            close: quote.regular_market_price.unwrap_or(0.0),
            volume: quote.regular_market_volume.unwrap_or(0),
            previous_close: quote.regular_market_previous_close,
        }
    }
}
