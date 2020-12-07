#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("unknown {0}")]
    Unknown(String),

    #[error("create table {0}")]
    CreateTable(#[from] rusoto_core::RusotoError<rusoto_dynamodb::CreateTableError>),

    #[error("update item {0}")]
    UpdateItem(#[from] rusoto_core::RusotoError<rusoto_dynamodb::UpdateItemError>),

    #[error("condition fail")]
    ConditionFail,
}
