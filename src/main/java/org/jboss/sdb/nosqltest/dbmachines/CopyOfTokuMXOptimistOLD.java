package org.jboss.sdb.nosqltest.dbmachines;

import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import org.bson.BasicBSONObject;
import org.bson.types.ObjectId;
import org.jboss.sdb.nosqltest.perf.ActionRecord;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoException;
import com.mongodb.WriteResult;

public class CopyOfTokuMXOptimistOLD extends TokuMX {
	
	@Override
	public HashMap<String, String> getKeysFromDB(int numberOfkeys) {
		
		HashMap<String, String> keys = new HashMap<String, String>();	
		
		if (collection == null)
			return null;
		
		long collectionCount = collection.getCount();

		if (collectionCount >= numberOfkeys){
					
			//Get Random documents from the DB and store them in the key map.
			while (keys.size() < numberOfkeys){
				final int rand = ThreadLocalRandom.current() .nextInt((int) collectionCount);
				BasicDBObject query = new BasicDBObject();
				BasicDBObject field = new BasicDBObject();
				field.put("_id", 1);
				keys.put((((BasicBSONObject) (collection.find(query,field).limit(-1).skip(rand)).next()).getString("_id")),"");
			}
			
		}else{
			System.out.println("check settings...");
		}
		
		return keys;
	}

	@Override
	public ActionRecord read(List<String> keys, int waitMillis) {
		
		ActionRecord record = new ActionRecord();
		
		
		try{
			db.command("beginTransaction");
			
			for (String key : keys){
				ObjectId key1 = new ObjectId(key);
				BasicDBObject searchQuery = new BasicDBObject("_id",key1);
	
				DBCursor cursor = collection.find(searchQuery);
	
				try{
					while(cursor.hasNext()) {
						cursor.next();
					}
				}finally{
					cursor.close();
				}
				
				//Wait for millis
				if (waitMillis > 0);
					waitBetweenActions(waitMillis);
				
			}
			
			
			db.command("endTransaction");
		}catch (MongoException e){
			record.setSuccess(false);
			try{
				db.command("rollbackTransaction");
			}catch (MongoException e2){
				System.out.println("serious error!");
			}
		}
		return record;
	}

	@Override
	public ActionRecord update(List<String> keys, int waitMillis) {
		final ActionRecord record = new ActionRecord();
		
		try{
			db.command("beginTransaction");
			for (String key : keys){
				ObjectId keyObj = new ObjectId(key);
				
				
				BasicDBObject newDocument = new BasicDBObject();
				newDocument.append("$set", new BasicDBObject().append("balance", 200));
				BasicDBObject searchQuery = new BasicDBObject().append("_id",keyObj);
			 
				collection.update(searchQuery, newDocument);
				
				//Wait for millis
				if (waitMillis > 0);
					waitBetweenActions(waitMillis);
			}
			db.command("commitTransaction");
		}catch (MongoException e){
			record.setSuccess(false);
			try{
				db.command("rollbackTransaction");
			}catch (MongoException e2){
				System.out.println("serious error!");
			}
		}
		return record;
	}

	@Override
	public ActionRecord insert(List<String> values, int waitMillis) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ActionRecord readModifyWrite(List<String> keys, int waitMillis) {
		final ActionRecord record = new ActionRecord();
		
		
		
		try{
			db.command("beginTransaction");
			for (String key : keys){
				ObjectId keyObj = new ObjectId(key);
				//BasicDBObject query = new BasicDBObject("_id",key1);
				
				BasicDBObject newDocument = new BasicDBObject().append("$inc", new BasicDBObject().append("increment", 1));
		 
				collection.update(new BasicDBObject().append("_id", keyObj), newDocument);
				
				//Wait for millis
				if (waitMillis > 0);
					waitBetweenActions(waitMillis);
			}
			db.command("commitTransaction");
		}catch (MongoException e){
			record.setSuccess(false);
			try{
				db.command("rollbackTransaction");
			}catch (MongoException e2){
				System.out.println("serious error!");
			}
		}
		return record;
	}

	@Override
	public ActionRecord balanceTransfer(String key1, String key2, int waitMillis) {
		final ActionRecord record = new ActionRecord();
		
		if ((key1 == null  || key1 =="") || (key2 ==null || key2 =="")){
			System.out.println("2 are keys required for balance transfer");
		}
		
		//Create usable keys
		ObjectId keyObj1 = new ObjectId(key1);
		ObjectId keyObj2 = new ObjectId(key2);
		
		//Setup a search query
		BasicDBObject searchQuery1 = new BasicDBObject("_id",keyObj1);
		BasicDBObject searchQuery2 = new BasicDBObject("_id",keyObj2);
		
		//Set the element to return
		BasicDBObject fields = new BasicDBObject();
		fields.put("balance", 1);
		
		try{
			
			//Set up the transaction
			BasicDBObject transaction = new BasicDBObject();
			transaction.append("beginTransaction", 1);
			transaction.append("isolation", "mvcc");
			db.command(transaction);
			
			//Get the cursor from the db
			int oldBalance1 = (Integer) collection.findOne(searchQuery1, fields).get("balance");
			int oldBalance2 = (Integer) collection.findOne(searchQuery2, fields).get("balance");
			//DBObject doc1 = collection.findOne(searchQuery1, fields);
			//DBObject doc2 = collection.findOne(searchQuery2, fields);
			
			//int oldBalance1 = (Integer) doc1.get("balance");
			//int oldBalance2 = (Integer) doc2.get("balance");
			
			//Wait for millis
			if (waitMillis > 0);
				waitBetweenActions(waitMillis);
				
			//Remove 50 from doc 1
			BasicDBObject set1 = new BasicDBObject();
			set1.append("$set", new BasicDBObject().append("balance", key1==key2? oldBalance1:oldBalance1-50));
			BasicDBObject queryOne = new BasicDBObject().append("_id",keyObj1);
			//queryOne.append("$eq", new BasicDBObject().append("balance", oldBalance1));//TODO is this needed?
			collection.update(queryOne, set1);
		
			
			//Add 50 to doc 2
			BasicDBObject set2 = new BasicDBObject();
			set2.append("$set", new BasicDBObject().append("balance", key1==key2? oldBalance2:oldBalance2+50));
			BasicDBObject searchQueryTwo = new BasicDBObject().append("_id",keyObj2);
			//searchQueryTwo.append("$eq", new BasicDBObject().append("balance", oldBalance1));//TODO is this needed?
			collection.update(searchQueryTwo, set2);
			
			BasicDBObject commitTransaction = new BasicDBObject();
			commitTransaction.append("commitTransaction", 1);
			db.command(commitTransaction);
			//System.out.println("Key 1: " + keyObj1.toString() + " OLD1 "+(oldBalance1) + " NEW: "+(oldBalance1-50));
			//System.out.println("Key 2: " + keyObj2.toString() + " OLD2 "+(oldBalance2) + "NEW"+(oldBalance1+50));
			
		}catch (MongoException e){
			//e.printStackTrace();
			//System.out.println("error!");
			record.setSuccess(false);
			try{
				BasicDBObject rollbackTransaction = new BasicDBObject();
				rollbackTransaction.append("rollbackTransaction", 1);
				db.command(rollbackTransaction);
				//db.command("rollbackTransaction");
			}catch (MongoException e2){
				System.out.println("serious error!");
			}
		}

		
		
		return record;
	}

	@Override
	public ActionRecord incrementalUpdate(List<String> keys, int waitMillis) {
		final ActionRecord record = new ActionRecord();
		
		try {
		db.command("beginTransaction");
		for (String key :keys){
			//Create usable keys
			ObjectId keyObj1 = new ObjectId(key);
			
			//Setup a search query
			BasicDBObject searchQuery1 = new BasicDBObject("_id",keyObj1);
			
			//Set the element to return
			BasicDBObject fields = new BasicDBObject();
			fields.put("increment", 1);
			
			//Get the current value from the db
			DBObject doc1 = collection.findOne(searchQuery1, fields);
			int doc1Mod = (Integer) doc1.get("increment");
			
			//Wait for millis
			if (waitMillis > 0);
				waitBetweenActions(waitMillis);
			
			//Add 1 to the value
			BasicDBObject newDocument = new BasicDBObject();
			newDocument.append("$set", new BasicDBObject().append("increment", doc1Mod +1));
			BasicDBObject searchQuery = new BasicDBObject().append("_id",keyObj1);
			searchQuery.append("$lte", new BasicDBObject().append("increment", doc1Mod));
			collection.update(searchQuery, newDocument);	
		}
		db.command("commitTransaction");
		System.out.println("end...");
		}
		catch (MongoException me){
			db.command("rollbackTransaction");
			me.printStackTrace();
		} catch (Exception e){
			e.printStackTrace();
		}
		
		return record;
	}

	@Override
	public ActionRecord writeLog(int numberToWrite, int waitMillis) {
		ActionRecord record = new ActionRecord();
		
		try{
			db.command("beginTransaction");
			for (int i = 0; i<numberToWrite; i++){
				DBCollection log = db.getCollection("log"+i);
				log.insert(new BasicDBObject("log", i));
				
				//Wait for millis
				if (waitMillis > 0);
					waitBetweenActions(waitMillis);
			}
			db.command("endTransaction");
		}catch (MongoException e){
			record.setSuccess(false);
			try{
				db.command("rollbackTransaction");
			}catch (MongoException e2){
				System.out.println("serious error!");
			}
		}
		return record;
	}

	@Override
	public ActionRecord readLog(int numberToRead, int waitMillis) {
		ActionRecord record = new ActionRecord();
		
		try {
			db.command("beginTransaction");
			for (int i = 0; i < numberToRead; i++){
				DBCollection log = db.getCollection("log"+i);
				DBCursor cursor = log.find().limit(1000);
				while (cursor.hasNext()) {
					//Scan through the log!
					cursor.next();
				}
				
				//Wait for millis
				if (waitMillis > 0);
					waitBetweenActions(waitMillis);
			
			}
			db.command("endTransaction");
		}catch (MongoException e){
			record.setSuccess(false);
			try{
				db.command("rollbackTransaction");
			}catch (MongoException e2){
				System.out.println("serious error!");
			}
		}
		
		return record;
	}

}
