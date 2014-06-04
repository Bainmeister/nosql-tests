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
import java.util.List;

import org.jboss.sdb.nosqltest.perf.ActionRecord;

import com.foundationdb.Database;
import com.foundationdb.FDB;
import com.foundationdb.Transaction;
import com.foundationdb.async.Function;
import com.foundationdb.tuple.Tuple;


/**
 * @author <a href="mailto:s.bain@newcastle.ac.uk">Simon Bain</a>
 *
 * A FoundationDB specific implementation of DBMachine. Runs the API as standard.
 */
public class FDBStandard implements DBMachine {

    //private Database db;
    private FDB fdb;
    private Database db;
	
	public void connectDB() {
      fdb = FDB.selectAPIVersion(200);
      db = fdb.open(); 
	}

	public void disconnectDB() {
		// Don't need to worry about this in FDB
		
	}
	
	public ActionRecord read(int keyLength, int transactionSize) {
		
		final ActionRecord record = new ActionRecord();
		
		//Get some keys for the transaction
		final List<String> keys = KeyGenerator.randomKeys(transactionSize, keyLength);
			
		//FDB API
	    return db.run(new Function<Transaction, ActionRecord>() {
	    		    	
	    	/** START TRANSACTION *********************/
		    public ActionRecord apply(Transaction tr) {
		    
		    	record.setAttemptsTaken(record.getAttemptsTaken()+1);
		    	
		    	record.setStartMillis(System.nanoTime());
		    	//For every key in the list do a read in this transaction
		    	for(String key: keys )
		    		decodeInt(tr.get(Tuple.from("class", key).pack()).get());	
		    	
		    	record.setEndMillis(System.nanoTime());
		    	
				return record;
		    }
		    /** END TRANSACTION   *********************/
	    
	    });	
	
	}

	public ActionRecord write(int keyLength, int transactionSize) {
		
		//generate a list of keys up to the transaction size
		final List<String> keys = KeyGenerator.randomKeys(transactionSize, keyLength);
		
		final ActionRecord record = new ActionRecord();
		
		//Use the DB API to read a single line.
	    return db.run(new Function<Transaction, ActionRecord>() {
	    	
	    		    	
	    	/** START TRANSACTION *********************/
		    public ActionRecord apply(Transaction tr) {
		    	
		    	record.setAttemptsTaken(record.getAttemptsTaken()+1);
		    	
		    	record.setStartMillis(System.nanoTime());
		    	//For every key in the list do a read in this transaction
		    	for(String key: keys ){
		    		tr.set(Tuple.from("class", key).pack(), encodeInt(-1));
		    	}
		    	record.setEndMillis(System.nanoTime());
		    	
				return record;
		    }
		    /** END TRANSACTION   *********************/
	    
	    });	
	}

	public ActionRecord readModifyWrite(int keyLength, int transactionSize) {
		
		//generate a list of keys up to the transaction size
		final List<String> keys = KeyGenerator.randomKeys(transactionSize, keyLength);
		final ActionRecord record = new ActionRecord();
		
	    return db.run(new Function<Transaction, ActionRecord>() {
	    	
	    	/** START TRANSACTION *********************/
		    public ActionRecord apply(Transaction tr) {
		    	
		    	record.setAttemptsTaken(record.getAttemptsTaken()+1);
		    	
		    	record.setStartMillis(System.nanoTime());
		    	for(String key: keys ){
		    		tr.set(Tuple.from("class", key).pack(), encodeInt(decodeInt(tr.get(Tuple.from("class", key).pack()).get()) + 1));
		    	}
		    	record.setEndMillis(System.nanoTime());
		    	
				return record;
		    }
		    /** END TRANSACTION   *********************/
	    
	    });	
	}
	
    /**
     * encode and int ready for FDB storage
     * @param value
     * @return
     */
	private static byte[] encodeInt(int value) {
		byte[] output = new byte[4];
		ByteBuffer.wrap(output).putInt(value);	
		return output;
	}
	
	/**
	 * Decode a byte into an int from FDB storage.
	 * @param value
	 * @return
	 */
	private static int decodeInt(byte[] value) {
		if (value.length != 4)
			throw new IllegalArgumentException("Array must be of size 4");
		return ByteBuffer.wrap(value).getInt();
	 }

}
