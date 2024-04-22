/*
items : {
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
                  '$items', [itemDoc]
                ]
              }
            }
          }
*/

let DB_NAME = "blockchain";
let COL_NAME = "collection";
let ITEMS_PER_BUCKET = 4;
//Let MAX_NUM_RETRIES = 5;

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


