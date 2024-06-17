/*
* File: src\TimeZone\index.js
* Project: Omni-miscellaneous
* Author: Bizcloud Experts
* Date: 2023-05-24
* Confidential and Proprietary
*/
const AWS = require("aws-sdk");
const s3 = new AWS.S3();
const dynamoDb = new AWS.DynamoDB.DocumentClient();
const csv = require("csvtojson");

module.exports.handler = async (event) => {
    console.log("event:", JSON.stringify(event));
    try {
        // Get the S3 bucket and object key from the event
        const bucket = event.Records[0].s3.bucket.name;
        const key = event.Records[0].s3.object.key;

        // Get the CSV file from the S3 bucket
        const params = { Bucket: bucket, Key: key };
        const data = await s3.getObject(params).promise();
        const csvData = data.Body.toString("utf-8");

        // Convert the CSV data to JSON
        const jsonObj = await csv().fromString(csvData);

        // Split the records into batches of 25
        const batches = [];
        while (jsonObj.length > 0) {
            batches.push(jsonObj.splice(0, 25));
        }

        // Insert each batch of records into the DynamoDB table
        for (let i = 0; i < batches.length; i++) {
            console.log(`Inserting batch ${i + 1} of ${batches.length}`);
            await batchWrite(process.env.TIMEZONE_OFFSET_TABLE, batches[i]);
        }

        console.log("records are inserted successfully");
    } catch (error) {
        console.error(error);
    }
};

async function batchWrite(tableName, items) {
    const params = {
        RequestItems: {
            [tableName]: items.map((item) => ({
                PutRequest: {
                    Item: item,
                },
            })),
        },
    };
    try {
        await dynamoDb.batchWrite(params).promise();
    } catch (e) {
        console.error("Batch Write Error: ", e);
    }
}
