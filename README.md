# dynamodb-mutex-rs

Mutex Lock Using DynamoDB.

[Documentation](https://docs.rs/dynamodb-mutex)

## Examples

```rust
use dynamodb_mutex::{
    error::DynamoDbMutexError,
    DynamoDbMutex,
};
use dynamodb_mutex::rusoto_core::Region;

#[tokio::main]
async fn main() -> Result<(), DynamoDbMutexError> {
    let mutex = DynamoDbMutex::new(Region::UsEast1, 10000, 10000, 10000, None);
    //mutex.make_table().await?;
    let res = mutex.lock("test").await?;
    println!("{:?}", res);
    mutex.unlock("test", true).await?;
    Ok(())
}
```