pub mod error;
use crate::error::Error;
use chrono::prelude::*;
use rusoto_core::Region;
pub use rusoto_dynamodb;
use rusoto_dynamodb::{
    AttributeDefinition, AttributeValue, CreateTableInput, DynamoDb, DynamoDbClient,
    KeySchemaElement, ProvisionedThroughput, UpdateItemInput, UpdateItemOutput,
};
use std::collections::HashMap;

static TABLE_NAME: &str = "mutexes";
static TABLE_KEY: &str = "mutex_code";

fn make_key(mutex_code: &str) -> HashMap<String, AttributeValue> {
    let mut map = HashMap::new();
    insert_str_attribute(&mut map, "mutex_code", mutex_code);
    map
}

async fn update(
    client: &DynamoDbClient,
    update_item_inupt: UpdateItemInput,
) -> Result<UpdateItemOutput, Error> {
    match client.update_item(update_item_inupt).await {
        Ok(res) => Ok(res),
        Err(rusoto_core::RusotoError::Service(
            rusoto_dynamodb::UpdateItemError::ConditionalCheckFailed(_),
        )) => Err(Error::ConditionFail),
        Err(err) => Err(err.into()),
    }
}

fn insert_str_attribute(map: &mut HashMap<String, AttributeValue>, key: &str, value: &str) {
    map.insert(
        key.to_owned(),
        AttributeValue {
            s: Some(value.to_owned()),
            ..Default::default()
        },
    );
}

fn insert_num_attribute(map: &mut HashMap<String, AttributeValue>, key: &str, value: i64) {
    map.insert(
        key.to_owned(),
        AttributeValue {
            n: Some(value.to_string()),
            ..Default::default()
        },
    );
}

/// Mutex Struct
pub struct DynamoDbMutex {
    table_name: String,
    client: DynamoDbClient,
    done_after_milli_seconds: u64,
    failed_after_milli_seconds: u64,
    running_after_milli_seconds: u64,
}

impl DynamoDbMutex {
    pub fn new(
        region: Region,
        done_after_milli_seconds: u64,
        failed_after_milli_seconds: u64,
        running_after_milli_seconds: u64,
        table_name: Option<&str>,
    ) -> Self {
        Self {
            table_name: table_name.unwrap_or(TABLE_NAME).to_owned(),
            client: DynamoDbClient::new(region),
            done_after_milli_seconds,
            failed_after_milli_seconds,
            running_after_milli_seconds,
        }
    }

    pub async fn make_table(&self) -> Result<(), Error> {
        let input = CreateTableInput {
            attribute_definitions: vec![AttributeDefinition {
                attribute_name: TABLE_KEY.to_owned(),
                attribute_type: "S".to_owned(),
            }],
            billing_mode: Some("PROVISIONED".to_owned()),
            provisioned_throughput: Some(ProvisionedThroughput {
                read_capacity_units: 1,
                write_capacity_units: 1,
            }),
            table_name: self.table_name.clone(),
            key_schema: vec![KeySchemaElement {
                attribute_name: TABLE_KEY.to_owned(),
                key_type: "HASH".to_owned(),
            }],
            ..Default::default()
        };
        let _ = self.client.create_table(input).await?;
        Ok(())
    }

    pub async fn lock(&self, mutex_code: &str) -> Result<(), Error> {
        let now: DateTime<Utc> = Utc::now();
        let now_millis = now.timestamp_millis();

        let mut map = HashMap::new();
        insert_str_attribute(&mut map, ":condion_done_status", &DynamoDbMutexStatus::Done.to_string());
        insert_num_attribute(
            &mut map,
            ":condion_done_millis",
            now_millis - self.done_after_milli_seconds as i64,
        );
        insert_str_attribute(&mut map, ":condion_failed_status", &DynamoDbMutexStatus::Failed.to_string());
        insert_num_attribute(
            &mut map,
            ":condion_failed_millis",
            now_millis - self.failed_after_milli_seconds as i64,
        );
        insert_str_attribute(&mut map, ":condion_running_status", &DynamoDbMutexStatus::Running.to_string());
        insert_num_attribute(
            &mut map,
            ":condion_running_millis",
            now_millis - self.running_after_milli_seconds as i64,
        );
        insert_str_attribute(&mut map, ":update_status", &DynamoDbMutexStatus::Running.to_string());
        insert_num_attribute(&mut map, ":now", now_millis);

        let condition = String::from("attribute_not_exists(mutex_status) OR mutex_status = :condion_done_status AND updated_at <= :condion_done_millis OR mutex_status = :condion_failed_status AND updated_at <= :condion_failed_millis OR mutex_status = :condion_running_status AND updated_at <= :condion_running_millis");

        let input = UpdateItemInput {
            key: make_key(mutex_code),
            table_name: self.table_name.clone(),
            condition_expression: Some(condition),
            update_expression: Some(
                "SET mutex_status = :update_status, updated_at = :now".to_owned(),
            ),
            expression_attribute_values: Some(map),
            return_values: Some(String::from("NONE")),
            ..Default::default()
        };
        let _ = update(&self.client, input).await?;
        Ok(())
    }

    pub async fn unlock(&self, mutex_code: &str, status: DynamoDbMutexStatus) -> Result<(), Error> {
        let now: DateTime<Utc> = Utc::now();
        let now_millis = now.timestamp_millis();

        let mut map = HashMap::new();
        insert_str_attribute(&mut map, ":condion_status", &DynamoDbMutexStatus::Running.to_string());
        insert_str_attribute(&mut map, ":update_status", &status.to_string());
        insert_num_attribute(&mut map, ":now", now_millis);

        let condition = String::from("mutex_status = :condion_status");

        let input = UpdateItemInput {
            key: make_key(mutex_code),
            table_name: self.table_name.clone(),
            condition_expression: Some(condition),
            update_expression: Some(
                "SET mutex_status = :update_status, updated_at = :now".to_owned(),
            ),
            expression_attribute_values: Some(map),
            return_values: Some(String::from("NONE")),
            ..Default::default()
        };
        let _ = update(&self.client, input).await?;
        Ok(())
    }
}

/// Mutex Status
pub enum DynamoDbMutexStatus {
    Running,
    Done,
    Failed,
}

impl ToString for DynamoDbMutexStatus {
    fn to_string(&self) -> String {
        match self {
            Self::Running => "RUNNING",
            Self::Done => "DONE",
            Self::Failed => "FAILED",
        }.to_owned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn it_works() -> Result<(), Error> {
        let mutex = DynamoDbMutex::new(Region::UsEast1, 10000, 10000, 10000, None);
        //mutex.make_table().await?;
        mutex.lock("test").await?;
        mutex.unlock("test", DynamoDbMutexStatus::Done).await?;
        Ok(())
    }
}
