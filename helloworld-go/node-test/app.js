// Load the AWS SDK for Node.js
var AWS = require('aws-sdk');
var csv = require('csvtojson');
var S3 = new AWS.S3({apiVersion: '2006-03-01'});
var params = {Bucket: 'testperf-bucket', Key: 'test.csv'};
var params1000 = {Bucket: 'testperf-bucket', Key: 'test-1000.csv'};
var params2000 = {Bucket: 'testperf-bucket', Key: 'test-2000.csv'};
var params5000 = {Bucket: 'testperf-bucket', Key: 'test-5000.csv'};
var params10000 = {Bucket: 'testperf-bucket', Key: 'test-10000.csv'};

let response;

/**
 *
 * Event doc: https://docs.aws.amazon.com/apigateway/latest/developerguide/set-up-lambda-proxy-integrations.html#api-gateway-simple-proxy-for-lambda-input-format
 * @param {Object} event - API Gateway Lambda Proxy Input Format
 *
 * Context doc: https://docs.aws.amazon.com/lambda/latest/dg/nodejs-prog-model-context.html 
 * @param {Object} context
 *
 * Return doc: https://docs.aws.amazon.com/apigateway/latest/developerguide/set-up-lambda-proxy-integrations.html
 * @returns {Object} object - API Gateway Lambda Proxy Output Format
 * 
 */
exports.lambdaHandler = async (event, context) => {

    try {
        let S3 = new AWS.S3({apiVersion: '2006-03-01'});

        let data = async function() {
            // get csv file and create stream
            const stream = S3.getObject(params).createReadStream();
            // convert csv file (stream) to JSON format data
            const json = await csv().fromStream(stream);
            
            return json;
        }

        let data1000 = async function() {
            // get csv file and create stream
            const stream = S3.getObject(params1000).createReadStream();
            // convert csv file (stream) to JSON format data
            const json = await csv().fromStream(stream);
            
            return json;
        }

        let data2000 = async function() {
            // get csv file and create stream
            const stream = S3.getObject(params2000).createReadStream();
            // convert csv file (stream) to JSON format data
            const json = await csv().fromStream(stream);
            
            return json;
        }

        let data5000 = async function() {
            // get csv file and create stream
            const stream = S3.getObject(params5000).createReadStream();
            // convert csv file (stream) to JSON format data
            const json = await csv().fromStream(stream);
            
            return json;
        }

        let data10000 = async function() {
            // get csv file and create stream
            const stream = S3.getObject(params10000).createReadStream();
            // convert csv file (stream) to JSON format data
            const json = await csv().fromStream(stream);
            
            return json;
        }

        let csvData = await data();
        let csvData1000 = await data1000();
        let csvData2000 = await data2000();
        let csvData5000 = await data5000();
        let csvData10000 = await data10000();

        // const ret = await axios(url);
        response = {
            'statusCode': 200,
            'body': JSON.stringify({
                'count200' : csvData.length,
                'count1000' : csvData1000.length,
                'count2000' : csvData2000.length,
                'count5000' : csvData5000.length,
                'count10000' : csvData10000.length,
            })
        }
    } catch (err) {
        console.log(err);
        return err;
    }

    return response

}
