# higgins

## Introduction 

`higgins` is currently a very simple implementation of a streaming platform that has some experimental features: 

- Uses Apache Arrow as the underlying data model, which requires the streams be schema'd
- Has built in support for transformational functions. 
- Allows querying values from a stream given a partition key and offset. 
- Built in joins. 
- Storage system externalized (primarily supporting object storage).
- Requires a simplified deployment model (use of internal consensus protocol for no reliance on external metadata store).

## Getting started

Simply start the client: 

```cargo run```

This will automatically interpret config.yaml as the initial configuration file which will set up two streams: 

`update_customer`: ingests update customer events to adjust a customer's data. 
`customer`: reduces update_customer's stream to the customer view. 


## License

This project is licensed under both an Apache 2.0 license.