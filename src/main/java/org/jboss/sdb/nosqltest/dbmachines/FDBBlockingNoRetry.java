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
import com.foundationdb.FDBException;
import com.foundationdb.Transaction;
import com.foundationdb.async.Function;
import com.foundationdb.async.Future;
import com.foundationdb.tuple.Tuple;


/**
 * @author <a href="mailto:s.bain@newcastle.ac.uk">Simon Bain</a>
 *
 * A FoundationDB specific implementation of DBMachine. Runs the API as standard.
 */
public class FDBBlockingNoRetry implements DBMachine {

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
		final List<String> keys = KeyGenerator.randomKeys(transactionSize, keyLength);
		
		Transaction tr1 = db.createTransaction();
		record.setSuccess(true);
		
		//Do it via blocking (GET) (Pessimistic locking)
		try {
		
			for(String key: keys ){
				decodeInt(tr1.get(Tuple.from("class", key).pack()).get());
			}
			
			//Blocks until a value is set on this Future and returns it.
			tr1.commit().get();
			record.setAttemptsTaken(record.getAttemptsTaken()+1);
			
		}catch (FDBException e){

			//Fail!
			record.setSuccess(false);
			
		}
		
		return record;
	
	}

	public ActionRecord write(int keyLength, int transactionSize) {
		
		//generate a list of keys up to the transaction size
		final List<String> keys = KeyGenerator.randomKeys(transactionSize, keyLength);
		
		final ActionRecord record = new ActionRecord();
		
		Transaction tr1 = db.createTransaction();
		record.setSuccess(true);
		
		//Do it via blocking (GET) (Pessimistic locking)
		try {
		
			for(String key: keys ){
				tr1.set(Tuple.from("class", key).pack(), encodeInt(-1));
			}
			
			//Blocks until a value is set on this Future and returns it.
			tr1.commit().get();
			record.setAttemptsTaken(record.getAttemptsTaken()+1);
			
		}catch (FDBException e){

			//Fail!
			record.setSuccess(false);
			
		}
		
		return record;
	}

	public ActionRecord readModifyWrite(int keyLength, int transactionSize) {
		
		
		//TODO this needs finishing. 
		//generate a list of keys up to the transaction size
		final List<String> keys = KeyGenerator.randomKeys(transactionSize, keyLength);
		final ActionRecord record = new ActionRecord();
		
		Transaction tr1 = db.createTransaction();
		record.setSuccess(true);
		
		//Do it via blocking (GET) (Pessimistic locking)
		try {
		
			for(String key: keys ){
	    		tr1.set(Tuple.from("class", key).pack(), encodeInt(decodeInt(tr1.get(Tuple.from("class", key).pack()).get()) + 1));
			}
			
			//Blocks until a value is set on this Future and returns it.
			tr1.commit().get();
			record.setAttemptsTaken(record.getAttemptsTaken()+1);
			
		}catch (FDBException e){

			//Fail!
			record.setSuccess(false);
			
		}
		
		return record;
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
