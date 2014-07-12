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

import org.jboss.sdb.nosqltest.perf.ActionRecord;

import com.foundationdb.Database;
import com.foundationdb.FDB;
import com.foundationdb.FDBException;
import com.foundationdb.Transaction;
import com.foundationdb.tuple.Tuple;


/**
 * @author <a href="mailto:s.bain@newcastle.ac.uk">Simon Bain</a>
 *
 * A FoundationDB specific implementation of DBMachine. Runs the API as standard.
 */
public class FoundationDBASyncNoRetry extends FoundationDB{


	public ActionRecord insert(List<String> values) {
		// TODO Auto-generated method stub
		return null;
	}

	public ActionRecord writeLog(int numberToWrite) {
		// TODO Auto-generated method stub
		return null;
	}

	public ActionRecord read(List<String> keys, int waitMillis) {
		final ActionRecord record = new ActionRecord();
		//final List<String> keys = KeyGenerator.randomKeys(transactionSize, keyLength);
		
		Transaction tr1 = db.createTransaction();
		record.setSuccess(true);
		
		//Do it via blocking (Map) 
		try {
		
			for(String key: keys ){
				decodeInt(tr1.get(Tuple.from("class", key).pack()).get());
			}
			
			//Async
			tr1.commit().map(null);
			record.setAttemptsTaken(record.getAttemptsTaken()+1);
			
		}catch (FDBException e){

			//Fail!
			record.setSuccess(false);
			
		}
		
		return record;
	}

	public ActionRecord update(List<String> keys, int waitMillis) {
		//generate a list of keys up to the transaction size
		//final List<String> keys = KeyGenerator.randomKeys(transactionSize, keyLength);
		
		final ActionRecord record = new ActionRecord();
		
		Transaction tr1 = db.createTransaction();
		record.setSuccess(true);
		
		//Do it via blocking (map) 
		try {
		
			for(String key: keys ){
				tr1.set(Tuple.from("class", key).pack(), encodeInt(-1));
			}
			
			//Async
			tr1.commit().map(null);
			record.setAttemptsTaken(record.getAttemptsTaken()+1);
			
		}catch (FDBException e){

			//Fail!
			record.setSuccess(false);
			
		}
		
		return record;
	}

	public ActionRecord insert(List<String> values, int waitMillis) {
		// TODO Auto-generated method stub
		return null;
	}

	public ActionRecord readModifyWrite(List<String> keys, int waitMillis) {
		//generate a list of keys up to the transaction size
				//final List<String> keys = KeyGenerator.randomKeys(transactionSize, keyLength);
				final ActionRecord record = new ActionRecord();
				
				Transaction tr1 = db.createTransaction();
				record.setSuccess(true);
				
				//Do it via blocking (map) 
				try {
				
					for(String key: keys ){
			    		tr1.set(Tuple.from("class", key).pack(), encodeInt(decodeInt(tr1.get(Tuple.from("class", key).pack()).get()) + 1));
					}
					
					//Async
					tr1.commit().map(null);
					record.setAttemptsTaken(record.getAttemptsTaken()+1);
					
				}catch (FDBException e){

					//Fail!
					record.setSuccess(false);
					
				}
				
				return record;
	}

	public ActionRecord balanceTransfer(String key1, String key2, int waitMillis) {
		// TODO Auto-generated method stub
		return null;
	}

	public ActionRecord incrementalUpdate(List<String> keys, int waitMillis) {
		// TODO Auto-generated method stub
		return null;
	}

	public ActionRecord writeLog(int numberToWrite, int waitMillis) {
		// TODO Auto-generated method stub
		return null;
	}

	public ActionRecord readLog(int numberToRead, int waitMillis) {
		// TODO Auto-generated method stub
		return null;
	}

}
