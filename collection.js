

let DB_NAME = "blockchain";
let COL_NAME = "collection";

use(DB_NAME);

let COL = db.getCollection(COL_NAME);

function createIndexes() {
	COL.createIndex({colNum: 1, itemId: 1}, {unique: true});
}


function insertData() {

	let colDoc = {
		_id: 123, // collection id
		colNum: 1,
		colName: "collection one",
		numItems: 0,
		items: []
	};

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

	COL.insertOne(colDoc);
	items.forEach(item => insertItem(item));
}

function createCollection() {
	COL.drop();
	createIndexes();
}


function insertItem(itemDoc) {
	let existingItemFilter = {
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
	let updateAggregation = [
  {
    '$set': {
			numItems: {
				$let : {
					vars : {
						existingItem: existingItemFilter
					},
					in: {
					 '$add': [
						'$numItems',
						{$cond : {if : "$$existingItem", then: 0, else: 1}}
					 ]
				  }
				}
			},
			'items': {
      '$let': {
        'vars': {
          'existingItem': existingItemFilter
        }, 
        'in': {
          '$cond': {
            'if': '$$existingItem', 
            'then': {
              '$map': {
                'input': '$items', 
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
        }
      }
    }
	}
];


	printjson(updateAggregation);
	COL.updateOne({colNum: 1},
								updateAggregation
							 );
}

function test() {
	createCollection();
	
	insertData();
}

test();
