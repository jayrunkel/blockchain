/*
	================================================================
	collection.js
	================================================================

	Description:
	This file demonstrates how to use a MongoDB collection to implement
	a collection (in the mathematical sense) of items that has the
	following properties:
	- collections and items are identified by unique ids.
	- when an item is added to a collection, if it already exists then
	  the attributes of the new version of the item are added or replace
		the existing item attributes. (Original attributes not references
		by the new item remain on the itme in the collection.
	- a collection could have > 10K items
	- inserting an item, retrieving an item, getting the total count of
	  items in a collection	should be extremely fast (not require large
	  aggregations).
	- to allow multiple threads to simultaneously insert items into the same
	  collection (or update them), the implementation avoids
		transactions on a common collection document. (This implementation
		does not use multi-document transactions.) 
		
  The implementation of the (mathematical) collection uses a MongoDB
	collection such that:
	- each collection is stored in a MongoDB document
	- all collections are stored in the same MongoDB collection
	- the items in a collection are stored on an array (items) on the
   	collection document
	- large collections (those with a large number of items) are
	  bucketed using multiple collection documents to keep the item
		array length manageable.
	- an updateOne method with the update operation defined by an
    aggregation pipeline is used to define the following logic:
	  + create a new collection document when none exists
		+ insert the new item in the items array, if it isn't in the collection 
	  + update the item in the items array, if it already exists
		+ create a new bucket if the existing bucket is full (has the
	    maximum number of items

	Designed to run in mongosh.


*/


let DB_NAME = "blockchain";
let COL_NAME = "collection";
let ITEMS_PER_BUCKET = 4;


use(DB_NAME);
let COL = db.getCollection(COL_NAME);

// Unique indexes may not be required in production, but they provide error checking during development
function createIndexes() {
	COL.createIndex({colNum: 1, "items.itemId": 1}, {unique: true});
}


function insertData() {

	let colId = 123;
	let colNum = 1;
	let colName = "collection one";

	let itemOneId = ObjectId();
	let itemTwoId = ObjectId();
	let itemFourId = ObjectId();
	
	let items = [
		{
			itemId: ObjectId(),
			itemNum: 0,
			itemName: "item 0",
		},
		{
			itemId: itemOneId,
		},
		{
			itemId: itemTwoId,
		},
		{
			itemId: ObjectId(),
			itemNum: 3,
			itemName: "item 3",
		},
		{
			itemId: itemFourId,
		},
		{
			itemId: ObjectId(),
			itemNum: 5,
			itemName: "item 5",
		},
		{
			itemId: itemOneId,
			itemNum: 1,
			itemName: "item 1",
		},
		{
			itemId: itemFourId,
			itemNum: 4,
			itemName: "item 4",
		},
		{
			itemId: itemTwoId,
			itemNum: 2,
			itemName: "item 2",
		},
	];

	items.forEach(item => insertItem(colId, colNum, colName, item));
}

function createCollection() {
	COL.drop();
	createIndexes();
}


function insertItem(colId, colNum, colName, itemDoc) {

	let existingItemFilter = 
			{
		$arrayElemAt : [
			{
				'$filter': {
					'input': '$items', 
					'cond': {
						'$eq': ['$$this.itemId', itemDoc.itemId]
					}
				}
			},
			0
		]
  };

	let itemCalcExpr =
			{
				$let : {
					vars : {
						existingItem : existingItemFilter
					},
					in: 
				{
          '$cond': {
            'if': '$$existingItem', 
            'then': {
              '$map': {
                'input': {$ifNull : ['$items', []]}, 
                'in': {
                  '$cond': {
                    'if': {
                      '$eq': ['$$existingItem.itemId', '$$this.itemId']
                    }, 
                    'then': {
                      '$mergeObjects': ['$$this', itemDoc]
                    }, 
                    'else': '$$this'
                    }
                  }
                }
              }, 
              'else': {
                '$concatArrays': [
                  {$ifNull: ['$items', []]}, [itemDoc]
                ]
              }
            }
				}
				}
			};
	let updateAggregation = [
  {
    '$set': {
			numItems: {
					 $add: [
						 {$ifNull : ['$numItems', 0]},
						 {$cond : {if : existingItemFilter, then: 0, else: 1}}
					 ]
			},
			items : itemCalcExpr, //{$concatArrays : [{$ifNull : ["$items", []]}, [itemDoc]]},
			colId : {$ifNull : ["$colId", colId]},
			colNum : {$ifNull : ["$colNum", colNum]},
			colName : {$ifNull : ["$colName", colName]},
			}

	}
		];


//	printjson(updateAggregation);
	let updateResult = COL.updateOne({$and : [{colNum: 1},
																		{"$or" : [{"items.itemId": itemDoc.itemId},
																						{numItems: {$lt:  ITEMS_PER_BUCKET}}]}]},
																	 updateAggregation,
																	 {upsert: true}
																	);
}

function getItem(colNum, itemNum) {
	let result = COL.aggregate([
  {
    '$match': {
      'colNum': colNum, 
      'items.itemNum': itemNum
    }
  }, {
    '$replaceRoot': {
      'newRoot': {
        '$first': {
          '$filter': {
            'input': '$items', 
            'cond': {
              '$eq': [
                '$$this.itemNum', itemNum
              ]
            }
          }
        }
      }
    }
  }
	]).toArray();

	return result;
}

function getCount(colNum) {
	let count = COL.aggregate([
  {
    '$match': {
      'colNum': colNum
    }
  }, {
    '$group': {
      '_id': null, 
      'total': {
        '$sum': '$numItems'
      }
    }
  }, {
    '$project': {
      '_id': 0
    }
  }
	]).toArray();

	return count;
	
}

function test() {
	createCollection();
	
	insertData();
	printjson(getItem(1, 4));
	printjson(getCount(1));
}

test();


