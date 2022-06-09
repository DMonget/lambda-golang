// Load the AWS SDK for Node.js
var AWS = require('aws-sdk');
var csv = require('csvtojson');
var S3 = new AWS.S3({apiVersion: '2006-03-01'});
var params = {Bucket: 'testperf-bucket', Key: 'test.csv'};

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

        let csvData = await data();

        // const ret = await axios(url);
        response = {
            'statusCode': 200,
            'body': JSON.stringify({
                csvData
            })
        }
    } catch (err) {
        console.log(err);
        return err;
    }

    return response

}
