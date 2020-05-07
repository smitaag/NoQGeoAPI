'use strict';
const AWS = require('aws-sdk');
AWS.config.update({ region: process.env.REGION });

const ddb = new AWS.DynamoDB();
const ddbGeo = require('dynamodb-geo');

//const config = new ddbGeo.GeoDataManagerConfiguration(ddb, process.env.GEO_TABLE);
const config = new ddbGeo.GeoDataManagerConfiguration(ddb, process.env.GEO_TABLE);
config.hashKeyLength = process.env.GEO_HASH_LEN;

const myGeoTableManager = new ddbGeo.GeoDataManager(config);

module.exports.handler = async (event, context) => {

  var params = event.queryStringParameters;
  var lat = parseFloat(params.lat);
  var lon = parseFloat(params.lon);
  var rad = parseFloat(params.rad);
  var results = null;

  var error = null;

  if (!lat || !lon || !rad) {
    return {
      statusCode: 422,
      body: JSON.stringify(
        {
          message: 'All mandatory fields are not supplied to perform Geo searching',
          code: 422
        },
        null,
        2
      ),
    };
  }

  await myGeoTableManager.queryRadius({
    RadiusInMeter: rad,
    CenterPoint: {
      latitude: lat,
      longitude: lon
    }
  }).then((locations) => {
    results = locations;
  }).catch((ex) => {
    error = "Error occurred while finding geo entities: " + ex;
  });

  if (error == null) {
    return {
      statusCode: 200,
      body: JSON.stringify(
        {
          message: "Stores found",
          code: "SUCCESS",
          resp: results
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
          resp: results,

        },
        null,
        2
      ),
    };
  }

};