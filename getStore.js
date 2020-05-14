'use strict';

const AWS = require('aws-sdk')
//AWS.config.update({ region: process.env.REGION })
const db = new AWS.DynamoDB.DocumentClient({ region: process.env.REGION });

module.exports.handler = async (event, context) => {

  try {
    var geoHash = "";
    var entity_id = "";
    var geoRange = "";

    if (event && event.pathParameters && event.pathParameters.id) {
      entity_id = event.pathParameters.id;
    } else {
      return {
        statusCode: 422,
        body: JSON.stringify(
          {
            message: 'Geo-Entity id is not in the correct format',
            code: "INVALID_ARGUMENT"
          },
          null,
          2
        ),
      };
    }

    var entity_params = {
      ExpressionAttributeValues: {
        ':pk': entity_id,
        ':sk': "GEO#"
      },
      KeyConditionExpression: 'pk1 = :pk and begins_with(sk1, :sk)',
      TableName: process.env.ENTITY_TABLE
    };

    await db.query(entity_params).promise()
      .then(result => {
        if (result && result.Items) {
          var entity_geo = result.Items[0];

          //sk1 should be "GEO#GR#345987"
          var arr = entity_geo.sk1.split("#");

          if (arr.length == 3) {
            geoHash = arr[2];

            //geoRange be of format "GR#Name#UUID"
            geoRange = arr[1] + "#" + entity_geo.d1 + "#" + entity_id;
          }
        }
      })
      .catch(ex => {
        console.log("Error reading the entity: " + ex);
        error = ex;
      });

    const geo_params = {
      TableName: process.env.GEO_TABLE,
      Key: {
        'hashKey': geoHash,
        'rangeKey': geoRange
      },
    };

    var store = null;
    var error;

    await db.get(geo_params).promise()
      .then(result => {
        if (result && result.Item) {
          var item = result.Item;
          var geo = JSON.parse(item.geoJson);
          item.geohash = 0;//setting it to 0 as it is not required to be exposed to client and also deserialization in failing

          item.lat = geo.coordinates[1];
          item.lon = geo.coordinates[0];
          item.geoJson = null;
          store = item;
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
            message: "Store not found with id: " + geoHash + "#" + geoRange,
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
            message: "Error geting the entity: " + geoHash + "#" + geoRange + " - Error: " + error,
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
