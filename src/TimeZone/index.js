const AWS = require('aws-sdk');
const s3 = new AWS.S3();
const dynamoDb = new AWS.DynamoDB.DocumentClient();
const csv = require('csv-parser');
const stream = require('stream');

module.exports.handler = async (event) => {
    console.log("event:", JSON.stringify(event));
    try {
        // Get the S3 bucket and object key from the event
        const bucket = event.Records[0].s3.bucket.name;
        const key = event.Records[0].s3.object.key;

        // Get the CSV file from the S3 bucket
        const params = { Bucket: bucket, Key: key };
        const data = await s3.getObject(params).promise();
        const csvData = data.Body.toString('utf-8');
        // Parse the CSV data
        const rows = [];
        const parser = csv();
        parser.on('data', (row) => rows.push(row));
        parser.on('end', async () => {
            console.log("rows:", rows);
            // Insert/update records into the DynamoDB table
            const batchSize = 25;
            for (let i = 0; i < rows.length; i += batchSize) {
                console.log("length",rows.length)
                const batchItems = rows.slice(i, i + batchSize);
                try {
                    const params = {
                        RequestItems: {
                            [process.env.TIMEZONE_OFFSET_TABLE]: batchItems.map((item) => ({
                                PutRequest: { Item: item }
                            }))
                        }
                    };
                    console.log("params:",params);
                    const result = await dynamoDb.batchWrite(params).promise();
                    console.log("result:",result);
                } catch (error) {
                    console.info(error);
                }
            }
            console.log("records are inserted successfully");
        });
        parser.on('error', (error) => console.error(error));
        const bufferStream = new stream.PassThrough();
        bufferStream.end(csvData);
        bufferStream.pipe(parser);
    } catch (error) {
        console.info(error);
    }
};
