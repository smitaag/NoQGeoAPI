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

function constructUpdateExpression(obj, expAtrValues) {
  var exp = "SET ";
  var count = 0;
  for (const [key, val] of Object.entries(obj)) {
    if (count != 0) {
      exp += ", " + key + " = :" + key;
    }
    else {
      exp = exp + key + " = :" + key;
    }

    expAtrValues[":" + key] = { 'S': val };

    count++;
  }

  return exp;
}

function checkPropertyChanges(oldObj, newObj) {
  var objWithNewChangedProperties = {};
  for (const [key, val] of Object.entries(newObj)) {
    if (oldObj[key]) {
      if (val != oldObj[key]) {
        objWithNewChangedProperties[key] = newObj[key];
      }
    }
    else {
      objWithNewChangedProperties[key] = newObj[key];
    }
  }

  return objWithNewChangedProperties;
}

module.exports.handler = async (event, context) => {

  try {

    //1. Get the store object
    //2. Use if the lat and lon has changed, then the current entry should be deleted and new entry to be inserted
    //3. If the lat-lon has not changed update the current entity

    var geoHash = "";
    var id = "";
    var error;
    var inpuStore = null;
    var store = null;
    var guid = "";
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

    inpuStore = JSON.parse(event.body);

    if (!inpuStore.tp || !inpuStore.nm || !inpuStore.adrs || !inpuStore.lcty || !inpuStore.lat
      || !inpuStore.lon || !inpuStore.ph || !inpuStore.mgr || !inpuStore.opnAt || !inpuStore.clsAt || !inpuStore.sltDur || !inpuStore.maxPerSlt
    ) {
      return {
        statusCode: 422,
        body: JSON.stringify(
          {
            message: 'All mandatory fields are not supplied to update the Store entry ',
            code: 422
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

    //1. Get the store object
    const params = {
      TableName: process.env.GEO_TABLE,
      Key: {
        'hashKey': geoHash,
        'rangeKey': geoRange
      },
    };

    await db.get(params).promise()
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
        error = ex;
        console.error("Error geting the entity: " + geoHash + "#" + geoRange + " - Error: " + error);
      });

    if (store == null && error == null) {
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

    //2. Check if the lat and lon has changed, then the current entry should be deleted and new entry to be inserted
    if (store != null && (inpuStore.lat != store.lat || inpuStore.lon != store.lon)) {
      // delete
      await db.delete(params).promise()
        .then(result => {

        })
        .catch(ex => {
          error = ex;
          console.error("Error deleting the entity: " + geoHash + "#" + geoRange + " - Error: " + error);
        });

      if (error == null) {
        //re-create now with the same id
        await myGeoTableManager.putPoint({
          RangeKeyValue: { S: geoRange }, // Use this to ensure uniqueness of the hash/range pairs.
          GeoPoint: { // An object specifying latitutde and longitude as plain numbers. Used to build the geohash, the hashkey and geojson data
            latitude: inpuStore.lat,
            longitude: inpuStore.lon
          },
          PutItemInput: { // Passed through to the underlying DynamoDB.putItem request. TableName is filled in for you.
            Item: { // The primary key, geohash and geojson data is filled in for you
              nm: { S: inpuStore.nm },
              tp: { S: inpuStore.tp },
              adrs: { S: inpuStore.adrs },
              lcty: { S: inpuStore.lcty },
              ph: { S: inpuStore.ph },
              mgr: { S: inpuStore.mgr },
              rgnm: { S: inpuStore.rgnm },
              opnAt: { S: inpuStore.opnAt },
              clsAt: { S: inpuStore.clsAt },
              sltDur: { S: inpuStore.sltDur },
              maxPerSlt: { S: inpuStore.maxPerSlt },
              clsdOn: { S: inpuStore.clsdOn },
              mxBil: { S: inpuStore.mxBil },
              gapDys: { S: inpuStore.gapDys },
              advnDys: { S: inpuStore.advnDys },
              brkSt: { S: inpuStore.brkSt },
              brkEn: { S: inpuStore.brkEn }
            }
          }
        }).promise()
          .then(function () { }).catch(function (msg) {
            error = msg;
            console.error("Error recreating the entity: " + geoHash + "#" + geoRange + " - Error: " + error);
          });
      }
    }
    else if (store != null) {
      //just update the existing store in DB as primary keys are not changed
      //check for the properties which are changed

      var changedProps = checkPropertyChanges(store, inpuStore);
      var expAtrValues = {};

      var updateExp = constructUpdateExpression(changedProps, expAtrValues);

      await myGeoTableManager.updatePoint({
        RangeKeyValue: { S: geoRange },
        GeoPoint: {
          latitude: inpuStore.lat,
          longitude: inpuStore.lon
        },
        UpdateItemInput: { // TableName and Key are filled in for you
          UpdateExpression: updateExp,
          ExpressionAttributeValues: expAtrValues
        }
      }).promise()
        .then(result => {
          console.log("Update called successfully!");
        })
        .catch(msg => {
          error = msg;
          console.error("Error updating the entity: " + geoHash + "#" + geoRange + " - Error: " + error);
        });
    }

    if (error == null) {
      return {
        statusCode: 200,
        body: JSON.stringify(
          {
            message: "Store updated",
            code: "SUCCESS",
            resp: {
              hash: myGeoTableManager.getGeoHashKey(inpuStore.lat, inpuStore.lon).toString(),
              id: inpuStore.tp + "#" + guid
            }
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
            message: "Error occurred while updating the store with id: " + geoHash + "#" + geoRange + " - Error: " + error,
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
