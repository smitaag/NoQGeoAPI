'use strict';

const AWS = require('aws-sdk')
//AWS.config.update({ region: process.env.REGION })
const db = new AWS.DynamoDB.DocumentClient({ region: process.env.REGION });

module.exports.handler = async (event, context) => {

  var geoHash = "";
  var id = "";

  if (event && event.pathParameters && event.pathParameters.id) {
    id = event.pathParameters.id;
    //expected format should be hash#type#GUID
    var arr = id.split("+");

    if (arr.length == 3) {
      geoHash = parseInt(arr[0]);
      id = arr[1] + "#" + arr[2];
    } else {
      return {
        statusCode: 422,
        body: JSON.stringify(
          {
            message: 'Store id is not in the correct format',
            code: "INVALID_ARGUMENT"
          },
          null,
          2
        ),
      };
    }
  }

  const params = {
    TableName: process.env.GEO_TABLE,
    Key: {
      'hashKey': geoHash,
      'rangeKey': id
    },
  };

  var store = {};
  var err;

  await db.get(params).promise()
    .then(result => {
      store = result;
    })
    .catch(error => {
      err = error;
    });


  return {
    statusCode: 200,
    body: JSON.stringify(
      {
        message: 'searchStores.js called !!',
        hashKey: geoHash,
        id: id,
        error: err,
        resp: store
      },
      null,
      2
    ),
  };

  // Use this code if you don't use the http event with the LAMBDA-PROXY integration
  // return { message: 'Go Serverless v1.0! Your function executed successfully!', event };
};
