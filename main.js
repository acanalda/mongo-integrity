var util = require('util'),
  mongo = require('mongodb'),
  MongoClient = mongo.MongoClient;

// Connection URL
var db_url = 'mongodb://localhost:27017';
  db_name = 'mongo_sync';

var mapper = {
  posts: {
    syncs: [
      {
        collection: 'posts_2',
        src_id: '_id',
        dst_id: 'a.b.post._id',
        dst_field: 'a.b.post',
        multi: true
      },
      {
        collection: 'posts_3',
        dst_id: 'a.b.post._id',
        dst_field: 'a.b.post',
        src_id: '_id',
        multi: false
      }
    ],
    onInsert: function(oplog){
      console.log('onInsert', oplog);
    },
    onUpdate: function(oplog){
      console.log('onUpdate', oplog);
    }
  }

};

var OplogSchema = function(obj){
  this.ts = obj.ts;
  this.insert = obj.op === 'i';
  this.update = obj.op === 'u';
  this.collection_name = obj.ns.split('.')[1];
  this.data = obj.o;
};

var Operation = function(db, map, oplog){
  var syncs = (Array.isArray(map.syncs))?map.syncs:[];

  this.oplog = oplog;
  this.onInsert = (oplog.insert && typeof map.onInsert === 'function')?map.onInsert:null;
  this.onUpdate = (oplog.update && typeof map.onUpdate === 'function')?map.onUpdate:null;
  this.sync_updates = syncs.map(function(sync_obj){
    return new Sync(db, sync_obj, oplog);
  })
};

var Sync = function(db, sync_obj, oplog){
  this.collection = db.collection(sync_obj.collection);;

  this.criteria = {};
  this.criteria[sync_obj.dst_id] = new mongo.ObjectID(oplog.data[sync_obj.src_id]);

  oplog.data[sync_obj.src_id] = new mongo.ObjectID(oplog.data[sync_obj.src_id]);
  this.update = {$set: {}};
  this.update.$set[sync_obj.dst_field] = oplog.data;

  this.options = {
    multi: (sync_obj.multi)?1:0
  }
};

// Use connect method to connect to the Server
MongoClient.connect(util.format('%s/%s', db_url, db_name), function(err, db) {
  if(err) return console.error(err.message);
  console.log('Connected to mongodb');

  var local_db = db.db('local'); // Connect to local database by reusing connection (System database)
  var now_ts = new local_db.bson_serializer.Timestamp(1, Math.floor((new Date().getTime()) / 1000)); // Make it paramater (now by default)
  var stream = local_db.collection('oplog.rs').find({ns: 'mongo_sync.posts', ts: {$gt: now_ts}}, {
    tailable: true,
    awaitdata: true,
    numberOfRetries: -1
  }).sort({$natural: -1}).stream();

  stream.on('data', function(document){
    var oplog = new OplogSchema(document),
      operation = getOperation(db, mapper, oplog);

    execOperation(operation);
  });

  //db.close();
});

function getOperation(db, mapper, oplog){
  var map = mapper[oplog.collection_name];

  if(map)
    return new Operation(db, map, oplog);

  return null;
}

function execOperation(operation){
  if(operation.onInsert) operation.onInsert(operation.oplog);
  if(operation.onUpdate) operation.onUpdate(operation.oplog);
  operation.sync_updates.forEach(function(sync_update){

    sync_update.collection.update(
      sync_update.criteria,
      sync_update.update,
      sync_update.options,
      function(){

      }
    );
  });
}