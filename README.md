## Handle Changing Reference Data In A Glue Streaming Job With High Availability

PROBLEM STATEMENT - Glue Streaming jobs process real time data from Kinesis data streams using micro-batches. The streaming functionality is built on top of Spark Structured Streaming framework and the foreachbatch method used to process micro-batches handles only a single stream of data. If the streaming job requires a lookup to a dynamic reference, the job will have to be restarted everytime there's a change which leads to poor availability and processing delays.

HOW ARE YOU SOLVING THE PROBLEM STATEMENT? - This blog explains a solution architecture that uses a combination of Dynamo DB streams (for reference data in DDB) or S3 event notifications (for reference data in S3) and Lambda functions to solve the problem of changing reference data.

CUSTOMER BENEFIT - Reference data changes will be applied in real-time to streaming sources without having to restart glue jobs. It also prevents redundant refreshes of the reference data thereby saving compute & network transfer time & costs.

TECHNICAL SOLUTION - This blog explains a solution architecture that uses a combination of Dynamo DB streams (for reference data in DDB) and Lambda functions to solve the problem of changing reference data. Every time there's a change in the reference, the Lambda will be triggered and a payload structured to look exactly the same as the streaming source data will be inserted into the source stream. This payload will contain a change flag which is monitored by the Glue Streaming job in every micro-batch. The moment it finds the change flag, the job will trigger a refresh of the reference data and the change will be available for all subsequent data streams.

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This library is licensed under the MIT-0 License. See the LICENSE file.

