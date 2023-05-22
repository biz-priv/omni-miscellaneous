const AWS = require('aws-sdk');
const s3 = new AWS.S3();
const dynamoDb = new AWS.DynamoDB.DocumentClient();
const csv = require('csv-parser');
const stream = require('stream');

exports.handler = async (event) => {
    console.log("event:",event);
    try {
        // Get the S3 bucket and object key from the event
        const bucket = event.Records[0].s3.bucket.name;
        const key = event.Records[0].s3.object.key;

        // Get the CSV file from the S3 bucket
        const params = { Bucket: bucket, Key: key };
        const data = await s3.getObject(params).promise();
        const csvData = data.Body.toString('utf-8')

        // Parse the CSV data
        const rows = [];
        const parser = csv();
        parser.on('data', (row) => rows.push(row));
        parser.on('end', () => {
            // Insert/update records into the DynamoDB table
            rows.forEach((row) => {
                const params = {
                    TableName: process.env.TIMEZONE_OFFSET_TABLE,
                    Item: row
                };
                dynamoDb.put(params).promise();
                console.log("records are inserted successfully");
            });
        });

        const bufferStream = new stream.PassThrough();
        bufferStream.end(csvData);
        bufferStream.pipe(parser);
    } catch (error) {
        console.error(error);
    }
};
