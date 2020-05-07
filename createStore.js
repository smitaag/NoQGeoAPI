'use strict';

const AWS = require('aws-sdk');
AWS.config.update({ region: process.env.REGION });

const ddb = new AWS.DynamoDB();
const ddbGeo = require('dynamodb-geo');

const config = new ddbGeo.GeoDataManagerConfiguration(ddb, process.env.GEO_TABLE);
// Pick a hashKeyLength appropriate to your usage
config.hashKeyLength = process.env.GEO_HASH_LEN;

const myGeoTableManager = new ddbGeo.GeoDataManager(config);
// For random numbers
const uuid = require('uuid');


module.exports.handler = async (event, context) => {


  var store = JSON.parse(event.body);

  if (!store.tp || !store.nm || !store.adrs || !store.lcty || !store.inApt || !store.lat
    || !store.lon || !store.ph || !store.mgr || !store.opnAt || !store.clsAt || !store.sltDur || !store.maxPerSlt
  ) {
    return {
      statusCode: 422,
      body: JSON.stringify(
        {
          message: 'All mandatory fields are not supplied to create the Store entry ',
          code: 422
        },
        null,
        2
      ),
    };
  }

  var isSuccess = false;
  var innerMessage = "";

  var id = uuid.v4();
  var hashKey = myGeoTableManager.getGeoHashKey(store.lat, store.lon);
  var range = store.tp + "+" + id;

  await myGeoTableManager.putPoint({
    RangeKeyValue: { S: range }, // Use this to ensure uniqueness of the hash/range pairs.
    GeoPoint: { // An object specifying latitutde and longitude as plain numbers. Used to build the geohash, the hashkey and geojson data
      latitude: store.lat,
      longitude: store.lon
    },
    PutItemInput: { // Passed through to the underlying DynamoDB.putItem request. TableName is filled in for you.
      Item: { // The primary key, geohash and geojson data is filled in for you
        tp: { S: store.tp },
        nm: { S: store.nm },
        adrs: { S: store.adrs },
        lcty: { S: store.lcty },
        inApt: { S: store.inApt },
        ph: { S: store.ph },
        mgr: { S: store.mgr },
        rgnm: { S: store.rgnm },
        opnAt: { S: store.opnAt },
        clsAt: { S: store.clsAt },
        sltDur: { S: store.sltDur },
        maxPerSlt: { S: store.maxPerSlt },
        clsdOn: { S: store.clsdOn },
        mxBil: { S: store.mxBil },
        gapDys: { S: store.gapDys },
        advnDys: { S: store.advnDys }
      }

      ,
      // ... Anything else to pass through to `putItem`, eg ConditionExpression
    }
  }).promise()
    .then(function () { isSuccess = true; }).catch(function (msg) { isSuccess = false; innerMessage = msg; });


  return {
    statusCode: isSuccess ? 201 : 500,
    body: JSON.stringify(
      {
        message: isSuccess ? "Store created!" : "Store creation failed - " + innerMessage,
        code: isSuccess ? "CREATED" : "SERVER_FAILURE",
        resp: isSuccess ? {
          hash: hashKey.toString(),
          id: range
        } : null,
      },
      null,
      2
    ),
  };
};
