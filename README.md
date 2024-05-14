# Description:

This file demonstrates how to use a MongoDB collection to implement
a collection (in the mathematical sense) of items that has the
following properties:

* collections and items are identified by unique ids.
*  when an item is added to a collection, if it already exists then
   the attributes of the new version of the item are added or replace
   the existing item attributes. (Original attributes not references
   by the new item remain on the itme in the collection.
* a collection could have > 10M items
* inserting an item, retrieving an item, getting the total count of
  items in a collection	should be extremely fast (not require large
  aggregations).
* to allow multiple threads to simultaneously insert items into the same
  collection (or update them), the implementation avoids
  transactions on a common collection document. (This implementation
  does not use multi-document transactions.) 
		
The implementation of the (mathematical) collection uses a MongoDB
collection such that:

* each collection is stored in a MongoDB document
* all collections are stored in the same MongoDB collection
* the items in a collection are stored on an array (items) on the collection document
* large collections (those with a large number of items) are
  bucketed using multiple collection documents to keep the item
  array length manageable.
* an updateOne method with the update operation defined by an
  aggregation pipeline is used to define the following logic: 
  
    * create a new collection document when none exists
    * insert the new item in the items array, if it isn't in the collection 
    * update the item in the items array, if it already exists
    * create a new bucket if the existing bucket is full (has the
       maximum number of items
