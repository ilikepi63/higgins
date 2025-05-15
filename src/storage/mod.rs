pub async fn produce() {

    // Custom implementation of BatchCoordinator.
let mut batch_coordinator = Arc::new(MyBatchCoordinator::new()); 
// Create an object store.
let mut object_store = Arc::new(MyObjectStore::new()); 

// Create the current produce request collection
let collection = ProduceRequestCollection::new();

collection.collect(    
    ProduceRequest {
        request_id: 1,
        topic: "example-topic".to_string(),
        partition: 1,
        data: "hello".as_bytes().to_vec(),
    },
)
.await
.unwrap();

let produce_response = flush(col, object_store.clone(), batch_coordinator.clone())
    .await
    .unwrap();

assert_eq!(produce_response.len(), 1);

let consume_response = consume(
    ConsumeRequest {
        topic: "example-topic".to_string(),
        partition: 1,
        offset: 0,
        max_partition_fetch_bytes: 0,
    },
    object_store,
    batch_coordinator,
)
.await;
}