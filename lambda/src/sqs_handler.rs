use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SqsMessage {
    #[serde(rename = "Records")]
    pub records: Vec<SqsRecord>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SqsRecord {
    #[serde(rename = "messageId")]
    pub message_id: String,
    #[serde(rename = "receiptHandle")]
    pub receipt_handle: String,
    pub body: String,
    pub attributes: HashMap<String, String>,
    #[serde(rename = "messageAttributes")]
    pub message_attributes: HashMap<String, MessageAttribute>,
    #[serde(rename = "md5OfBody")]
    pub md5_of_body: String,
    #[serde(rename = "eventSource")]
    pub event_source: String,
    #[serde(rename = "eventSourceARN")]
    pub event_source_arn: String,
    #[serde(rename = "awsRegion")]
    pub aws_region: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageAttribute {
    #[serde(rename = "stringValue")]
    pub string_value: Option<String>,
    #[serde(rename = "binaryValue")]
    pub binary_value: Option<String>,
    #[serde(rename = "dataType")]
    pub data_type: String,
}