'use strict';

const AWS = require('aws-sdk');
AWS.config.update({ region: process.env.REGION });

const db = new AWS.DynamoDB.DocumentClient({ region: process.env.REGION });

const ddb = new AWS.DynamoDB();
const ddbGeo = require('dynamodb-geo');

const config = new ddbGeo.GeoDataManagerConfiguration(ddb, process.env.GEO_TABLE);
// Pick a hashKeyLength appropriate to your usage
config.hashKeyLength = process.env.GEO_HASH_LEN;

const myGeoTableManager = new ddbGeo.GeoDataManager(config);
// For random numbers
const uuid = require('uuid');

module.exports.handler = async (event, context) => {

  try {

    var store = JSON.parse(event.body);

    if (!store.tp || !store.nm || !store.adrs || !store.lcty || !store.lat
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
    var range = store.tp + "#" + store.name + "#" + id;

    await myGeoTableManager.putPoint({
      RangeKeyValue: { S: range }, // Use this to ensure uniqueness of the hash/range pairs.
      GeoPoint: { // An object specifying latitutde and longitude as plain numbers. Used to build the geohash, the hashkey and geojson data
        latitude: store.lat,
        longitude: store.lon
      },
      PutItemInput: { // Passed through to the underlying DynamoDB.putItem request. TableName is filled in for you.
        Item: { // The primary key, geohash and geojson data is filled in for you
          nm: { S: store.nm },
          tp: { S: store.tp },
          adrs: { S: store.adrs },
          lcty: { S: store.lcty },
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
          advnDys: { S: store.advnDys },
          brkSt: { S: store.brkSt },
          brkEn: { S: store.brkEn }
        }
      }
    }).promise()
      .then(function () { isSuccess = true; }).
      catch(function (msg) {
        isSuccess = false;
        innerMessage = msg;
        console.error("Error creating Entity in the GEO table: " + hashKey + ":" + range + "Error - " + msg);
      });

    //now create the Entity_GEO entry in the Entity Table
    var sortKey = "GEO#" + store.tp + "#" + hashKey.toString();
    const params = {
      TableName: process.env.ENTITY_TABLE,
      Item: { "pk1": id, "sk1": sortKey, "d1": store.nm }
    };

    await db.put(params).promise().
      then(function () { isSuccess = true; }).
      catch(function (msg) {
        isSuccess = false;
        innerMessage = msg;
        console.error("Error creating Entity in the Entity table: " + hashKey + ":" + range + "Error - " + msg);
      });

    return {
      statusCode: isSuccess ? 201 : 500,
      body: JSON.stringify(
        {
          message: isSuccess ? "Store created!" : "Store creation failed - " + innerMessage,
          code: isSuccess ? "CREATED" : "SERVER_FAILURE",
          resp: isSuccess ? {
            id: id
          } : null,
        },
        null,
        2
      ),
    };

  } catch (ex) {
    console.error("Error occurred: " + ex);
  }
};
