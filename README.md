## Handle Changing Reference Data In A Glue Streaming Job With High Availability

PROBLEM STATEMENT - Glue Streaming jobs process real time data from Kinesis data streams using micro-batches. The streaming functionality is built on top of Spark Structured Streaming framework and the foreachbatch method used to process micro-batches handles only a single stream of data. If the streaming job requires a lookup to a dynamic reference, the job will have to be restarted everytime there's a change which leads to poor availability and processing delays.

HOW ARE YOU SOLVING THE PROBLEM STATEMENT? - This blog explains a solution architecture that uses a combination of Dynamo DB streams (for reference data in DDB) or S3 event notifications (for reference data in S3) and Lambda functions to solve the problem of changing reference data.

CUSTOMER BENEFIT - Reference data changes will be applied in real-time to streaming sources without having to restart glue jobs. It also prevents redundant refreshes of the reference data thereby saving compute & network transfer time & costs.

TECHNICAL SOLUTION - This blog explains a solution architecture that uses a combination of Dynamo DB streams (for reference data in DDB) and Lambda functions to solve the problem of changing reference data. Every time there's a change in the reference, the Lambda will be triggered and a change flag gets created. This flag will be monitored by the Glue Streaming job in every micro-batch. The moment it finds the change flag, the job will trigger a refresh of the reference data and the change will be available for all subsequent data streams.

SOLUTION OVERVIEW - 

![Real Time Lookup Patterns-v2-DDB of Solution Overview drawio](https://user-images.githubusercontent.com/11506905/150310854-a36de3ff-c514-4d98-821f-44b976a68434.png)

The workflow contains the following steps:

1.	A user or application updates or creates a new item in the DynamoDB table.
2.	DynamoDB Streams is used to identify changes in the reference data. 
3.	A Lambda function is invoked every time a change occurs in the reference data. 
4.	The Lambda function captures the event containing the changed record, adds a change flag, creates a JSON message that is structured similar to the messages in the source stream, and sends it to the source Kinesis data stream. 
5.	The AWS Glue job is designed to monitor the stream for this value in every micro-batch. The moment that it sees the change flag, AWS Glue initiates a refresh of the DynamoDB data before processing any further records in the stream.



DEMONSTRATION SETUP - 
Create resources with AWS CloudFormation
To deploy the solution, complete the following steps:

1.	Launch the CloudFormation stack - https://console.aws.amazon.com/cloudformation/home?#/stacks/new?stackName=real-time-streaming-lookup-demo-stack&templateURL=https://aws-bigdata-blog.s3.amazonaws.com/artifacts/BDB-1808-Changing-Reference-Data-In-Glue-Streaming-Job-With-High-Availability/template.yml
3.	Set up an Amazon Cognito user pool (https://awslabs.github.io/amazon-kinesis-data-generator/web/help.html) and test if you can access the KDG URL specified in the stack’s output tab. Furthermore, validate if you can log in to KDG using the credentials provided while creating the stack. You should now have the required resources available in your AWS account.
4.	Verify this list with the resources in the output section of the CloudFormation stack.

![real-time-architecture-diagram-Page-1 drawio](https://user-images.githubusercontent.com/11506905/150673751-c2a04037-9040-47d6-b953-8069cf9fd10c.png)

1.	A user or application updates or creates a new item in the DynamoDB table.
2.	DynamoDB Streams is used to identify changes in the reference data. 
3.	A Lambda function is invoked every time a change occurs in the reference data. 
4.	The Lambda function captures the event containing the changed record, creates a “change file” and places it in an Amazon S3 bucket.
5.	The AWS Glue job is designed to monitor the S3 prefix for this value in every micro-batch. The moment that it sees the change flag, AWS Glue initiates a refresh of the DynamoDB data before processing any further records in the stream.


AUTHOR - Jerome Rajan

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This library is licensed under the MIT-0 License. See the LICENSE file.
