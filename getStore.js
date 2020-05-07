'use strict';

const AWS = require('aws-sdk')
//AWS.config.update({ region: process.env.REGION })
const db = new AWS.DynamoDB.DocumentClient({ region: process.env.REGION });

module.exports.handler = async (event, context) => {

  try {

    var geoHash = "";
    var id = "";

    if (event && event.pathParameters && event.pathParameters.id) {
      id = event.pathParameters.id;
      //expected format should be hash#type#GUID
      var arr = id.split("+");

      if (arr.length == 3) {
        geoHash = parseInt(arr[0]);
        id = arr[1] + "+" + arr[2];
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

    var store = null;
    var error;

    await db.get(params).promise()
      .then(result => {
        if (result && result.Item) {
          result.Item.geohash = 0;//setting it to 0 as it is not required to be exposed to client and also deserialization in failing
          store = result.Item;
        }

      })
      .catch(ex => {
        console.log("Error reading the entity: " + ex);
        error = ex;
      });

    if (store == null) {
      return {
        statusCode: 404,
        body: JSON.stringify(
          {
            message: "Store not found with id: " + geoHash + "+" + id,
            code: "NOT_FOUND",
            resp: null
          },
          null,
          2
        ),
      };
    }


    if (error == null) {
      return {
        statusCode: 200,
        body: JSON.stringify(
          {
            message: "Store found",
            code: "SUCCESS",
            resp: store
          },
          null,
          2
        ),
      };
    }
    else {
      return {
        statusCode: 500,
        body: JSON.stringify(
          {
            message: "Error occurred while searching for stores: " + error,
            code: "SERVER_FAILURE",
            resp: null,

          },
          null,
          2
        ),
      };
    }
  }
  catch (ex) {
    console.error("Error occurred: " + ex);
  }
};
