#[derive(thiserror::Error, Debug)]
pub enum DynamoDbMutexError {
    #[error("create table {0}")]
    CreateTable(#[from] rusoto_core::RusotoError<rusoto_dynamodb::CreateTableError>),

    #[error("update item {0}")]
    UpdateItem(#[from] rusoto_core::RusotoError<rusoto_dynamodb::UpdateItemError>),

    #[error("delete item {0}")]
    DeleteItem(#[from] rusoto_core::RusotoError<rusoto_dynamodb::DeleteItemError>),

    #[error("fail db value")]
    FailDbValue,
}
