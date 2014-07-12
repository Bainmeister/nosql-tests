/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2013 Red Hat, Inc., and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.jboss.sdb.nosqltest.dbmachines;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.jboss.sdb.nosqltest.perf.ActionRecord;

import com.foundationdb.Database;
import com.foundationdb.FDB;
import com.foundationdb.KeyValue;
import com.foundationdb.Transaction;
import com.foundationdb.async.Function;
import com.foundationdb.tuple.Tuple;

/**
 * @author <a href="mailto:s.bain@newcastle.ac.uk">Simon Bain</a>
 *
 * A FoundationDB specific implementation of DBMachine. Runs the API as standard.
 */
public class FoundationDB implements DBMachine{

    //private Database db;
    private FDB fdb;
    Database db;
	
	public void connectDB() {
      fdb = FDB.selectAPIVersion(200);
      db = fdb.open(); 
	}

	public void disconnectDB() {
		// Don't need to worry about this in FDB
		
	}
		
    /**
     * encode and int ready for FDB storage
     * @param value
     * @return
     */
	byte[] encodeInt(int value) {
		byte[] output = new byte[4];
		ByteBuffer.wrap(output).putInt(value);	
		return output;
	}
	
	/**
	 * Decode a byte into an int from FDB storage.
	 * @param value
	 * @return
	 */
	int decodeInt(byte[] value) {
		if (value.length != 4)
			throw new IllegalArgumentException("Array must be of size 4");
		return ByteBuffer.wrap(value).getInt();
	 }

		
	public HashMap<String, String> getKeysFromDB(final int numberOfkeys) {
		
	    return db.run(new Function<Transaction, HashMap<String, String>>() {
	    	
	    	/** START TRANSACTION *********************/
		    public HashMap<String, String> apply(Transaction tr) {
		    
	    		HashMap<String, String> keys = new HashMap<String, String>();	

	    		for(KeyValue kv: tr.getRange(Tuple.from("testData").range())){
	    			keys.put(Tuple.fromBytes(kv.getKey()).getString(1),"");
	    		}
	
	    		return keys;
		   
		    }
		    /** END TRANSACTION   *********************/
	    
	    });		
	}

	public ActionRecord insert(List<String> values) {
		final ActionRecord record = new ActionRecord();
		return record;
	}

	public void addTable(String name) {
		// TODO Auto-generated method stub
	}

	public ActionRecord writeLog(int numberToWrite) {
		final ActionRecord record = new ActionRecord();
		return record;
	}

	public ActionRecord read(final List<String> keys, final int waitMillis) {
		
		final ActionRecord record = new ActionRecord();
		
		//FDB API
	    return db.run(new Function<Transaction, ActionRecord>() {
	    	
	    	//TODO failure is possible - add a check
	    	
	    	/** START TRANSACTION *********************/
		    public ActionRecord apply(Transaction tr) {
		    
		    	record.setAttemptsTaken(record.getAttemptsTaken()+1);
		    	
		    	//For every key in the list do a read in this transaction
		    	for(String key: keys ){
		    		decodeInt(tr.get(Tuple.from("testData", key).pack()).get());	
		    		
		    		try {
						Thread.sleep(waitMillis);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
		    		
		    	}
		    	
				return record;
		    }
		    /** END TRANSACTION   *********************/
	    
	    });	
	
	}

	public ActionRecord update(final List<String> keys, final int waitMillis) {

		//TODO failure is possible - add a check
		final ActionRecord record = new ActionRecord();
		
		//Use the DB API to update a single line.
	    return db.run(new Function<Transaction, ActionRecord>() {
	    	
	    		    	
	    	/** START TRANSACTION *********************/
		    public ActionRecord apply(Transaction tr) {
		    	
		    	record.setAttemptsTaken(record.getAttemptsTaken()+1);
		    	
		    	//For every key in the list do a read in this transaction
		    	for(String key: keys ){
		    		tr.set(Tuple.from("testData", key).pack(), encodeInt(-1));
		    		
		    		waitBetweenActions(waitMillis);
		    	}
		    	
				return record;
		    }
		    /** END TRANSACTION   *********************/
	    
	    });	
	}

	public ActionRecord insert(List<String> values, int waitMillis) {
		final ActionRecord record = new ActionRecord();
		return record;
	}

	public ActionRecord readModifyWrite(final List<String> keys, final int waitMillis) {

		final ActionRecord record = new ActionRecord();
				
	    return db.run(new Function<Transaction, ActionRecord>() {
	    	
	    	/** START TRANSACTION *********************/
		    public ActionRecord apply(Transaction tr) {
		    	
		    	record.setAttemptsTaken(record.getAttemptsTaken()+1);
		    	
		    	for(String key: keys ){
		    		tr.set(Tuple.from("testData", key).pack(), encodeInt(decodeInt(tr.get(Tuple.from("testData", key).pack()).get()) + 1));
		    		waitBetweenActions(waitMillis);
		    	}
		    	
				return record;
		    }
		    /** END TRANSACTION   *********************/
	    
	    });	
	}

	public ActionRecord balanceTransfer(final String key1, final String key2, final int waitMillis) {
		
		
	    return db.run(new Function<Transaction, ActionRecord>() {
	    	
	    	final ActionRecord record = new ActionRecord();
	    	
	    	/** START TRANSACTION *********************/
		    public ActionRecord apply(Transaction tr) {
		    	
		    	record.setAttemptsTaken(record.getAttemptsTaken()+1);
		    	
		    	
		    	//tr.set(Tuple.from("testData", key).pack(), encodeInt(decodeInt(tr.get(Tuple.from("testData", key).pack()).get()) + 1));
		    	//waitBetweenActions(waitMillis);
		    	
		    	
				return record;
		    }
		    /** END TRANSACTION   *********************/
	    
	    });	
	}

	public ActionRecord incrementalUpdate(List<String> keys, int waitMillis) {
		final ActionRecord record = new ActionRecord();
		return record;
	}

	public ActionRecord writeLog(int numberToWrite, int waitMillis) {
		final ActionRecord record = new ActionRecord();
		return record;
	}

	public ActionRecord readLog(int numberToRead, int waitMillis) {
		final ActionRecord record = new ActionRecord();
		return record;
	}
	
	public void waitBetweenActions(int millis){
		try {
			TimeUnit.MILLISECONDS.sleep(millis);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}	
	}
	
}
