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

	Sample document: 		
  {
    "_id": {
      "$oid": "66268d5ea752a41b67c62106"
    },
    "colNum": 1,
    "numItems": 4,
    "items": [
      {
        "itemId": {
          "$oid": "66268d5e00e0f763e77a61ec"
        },
        "itemNum": 0,
        "itemName": "item 0",
      },
      {
        "itemId": {
          "$oid": "66268d5e00e0f763e77a61e9"
        },
        "itemNum": 1,
        "itemName": "item 1"
      },
      {
        "itemId": {
          "$oid": "66268d5e00e0f763e77a61ea"
        },
        "itemNum": 2,
        "itemName": "item 2"
      },
      {
        "itemId": {
          "$oid": "66268d5e00e0f763e77a61ed"
        },
        "itemNum": 3,
        "itemName": "item 3"
      }
    ],
    "colId": 123,
    "colName": "collection one"
  }		

	Designed to run in mongosh.

	This code supports the following public operations:
	- createCollection - drops the mongoDB collection and adds required
                       indexes. 
	- insertItem - inserts an item into a collection. Creates
                 collection/bucket documents as necessary.
		
	- getItem - returns the requested item
	- getCount - returns the number of items in a collection							 

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


// Test function
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

	// has the value of non-null if their exists an item in the items
	// array that has the same itemId as itemDoc
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

	// Calculates the new value of the items array.
	// - If the item is already in the array new itemDoc and the
  //   existing item are merged
  // - If the item isn't in the array, itemDoc is appended to the end
  //   of items
	//
	// The logic here is designed to handle situations when a new
  // collection document needs to be created either because one
  // doesn't exist or a new one has to be created because the previous
  // bucket is full. The implication of this is that the code below
  // must do the right thing when the items array ("$items") has the
  // value of null
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
				 items : itemCalcExpr, 
				 colId : {$ifNull : ["$colId", colId]},
				 colNum : {$ifNull : ["$colNum", colNum]},
				 colName : {$ifNull : ["$colName", colName]},
			 }

		 }
	 ];

	// There are three possible scenario for updates:
	// 1. A collection document doesn't exist
	// 2. A collection document exists and there is space in the items
	//    array
	// 3. A collection document exists and there is not space in the
	//    items array
	
	 let updateResult = COL.updateOne({$and : [{colNum: 1},
																						 {
																							 "$or" : [
																								 {"items.itemId": itemDoc.itemId},
																								 {numItems: {$lt:
																														 ITEMS_PER_BUCKET}}
																							 ]
																						 }
																						]},
																		updateAggregation,
																		{upsert: true}
																	 );
 }

// Returns the specified item given the collection and item number.
// The $match stage selects the bucket that contains the item. The
// $replaceRoot operation makes the selected item the root of the
// document to be returned. The item is found using $filter. (Don't
// use $unwind, it is less efficient.)

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

// Counts the number of items, but suming up the number of items
// across the buckets for a collection.
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


