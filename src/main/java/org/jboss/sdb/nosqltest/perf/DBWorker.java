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
package org.jboss.sdb.nosqltest.perf;



import io.narayana.perf.Result;
import io.narayana.perf.Worker;

import java.util.concurrent.ThreadLocalRandom;

import org.jboss.sdb.nosqltest.dbmachines.DBMachine;
import org.jboss.sdb.nosqltest.dbmachines.FDBStandard;


/**
 * @author <a href="mailto:s.bain@newcastle.ac.uk">Simon Bain</a>
 *
 * A Worker that interacts with DBMachine to manipulate Databases. 
 * @param <T>
 */
public class DBWorker<T> implements Worker<T>{
	
	DBMachine machine;

	private long workTimeMillis;
	private long initTimemillis;
	private long finiTimeMillis;
	
	final public static int NOT_SET= 0;
	final public static int FDB = 10;
	final public static int FDB_NO_RETRY=11;
	final public static int FDB_COMPENSATION =12;
	final public static int MONGODB = 20;
	final public static int MONGODB_COMPENSATION = 20;
	
	private int keyLength;
	private int dbType;
	private int chanceOfRead; 
	private int chanceOfWrite; 
	private int chanceOfReadModifyWrite; 
	private int minTransactionSize;
	private int maxTransactionSize;
	
	DBWorker(int keyLength, 
				int dbType, 
				int chanceOfRead, 
				int chanceOfWrite, 
				int chanceOfReadModifyWrite, 
				int minTransactionSize,
				int maxTransactionSize){
		
		//Set the Parameters
		this.keyLength = keyLength;
		this.dbType = dbType;
		this.chanceOfRead = chanceOfRead; 
		this.chanceOfWrite = chanceOfWrite; 
		this.chanceOfReadModifyWrite = chanceOfReadModifyWrite; 
		this.minTransactionSize = minTransactionSize;
		this.maxTransactionSize = maxTransactionSize;
		
		//Set up the relevant Database Machine
		if (dbType == FDB){
			machine = new FDBStandard();
		}else if (dbType == FDB_NO_RETRY){
			//machine = new FDBNoRetry();
		}else if (dbType == FDB_COMPENSATION){
			//machine = new FDBCompensation();
		}else if (dbType == MONGODB){
			//machine = new MongoDBMachine();
		}			
		
		//Connect the DB
		machine.connectDB();
	}
	
	public T doWork(T context, int niters, Result<T> opts) {
    	
		//Get configuration
    	final int keyLength = this.keyLength;
    	final int transactionSize = ThreadLocalRandom.current().nextInt(this.maxTransactionSize)+1;  //Range 1 - Max rather than 0 to Max-1
    	    	
    	//Get Random number to assign task
    	final int rand1 = ThreadLocalRandom.current() .nextInt(100);
    	    	
    	ActionRecord record = new ActionRecord();

    	//Call the relevant method, depending upon ChanceOfProcess and the transactionSize
    	if (rand1< chanceOfRead){
    		//Reader
    		record = machine.read(keyLength, transactionSize);
    	}else if(rand1 < chanceOfWrite){
    		//Writer
    		record = machine.write(keyLength, transactionSize);
    	}else if(rand1 < chanceOfReadModifyWrite){
    		//Reader + Writer
    		record = machine.readModifyWrite(keyLength, transactionSize);
    	}
    	
    	long timeTaken  = (record.getEndMillis() - record.getStartMillis());
    	
    	System.out.println("Transaction Size: " + transactionSize + "  Attemps: "+ record.getAttemptsTaken() +" Start Time: "+ record.getStartMillis() +" Time Taken: "+ timeTaken);
    	
        workTimeMillis = System.currentTimeMillis();

        return null;
	}

	public void init() {
		initTimemillis = System.currentTimeMillis();
		
	}

	public void fini() {
		finiTimeMillis = System.currentTimeMillis();
		
	}

	/**
	 * @return the workTimeMillis
	 */
	public long getWorkTimeMillis() {
		return workTimeMillis;
	}

	/**
	 * @return the initTimemillis
	 */
	public long getInitTimemillis() {
		return initTimemillis;
	}

	/**
	 * @return the finiTimeMillis
	 */
	public long getFiniTimeMillis() {
		return finiTimeMillis;
	}

	/**
	 * @return the keyLength
	 */
	public int getKeyLength() {
		return keyLength;
	}

	/**
	 * @param keyLength the keyLength to set
	 */
	public void setKeyLength(int keyLength) {
		this.keyLength = keyLength;
	}

	/**
	 * @return the dbType
	 */
	public int getDbType() {
		return dbType;
	}

	/**
	 * @param dbType the dbType to set
	 */
	public void setDbType(int dbType) {
		this.dbType = dbType;
	}

	/**
	 * @return the chanceOfRead
	 */
	public int getChanceOfRead() {
		return chanceOfRead;
	}

	/**
	 * @param chanceOfRead the chanceOfRead to set
	 */
	public void setChanceOfRead(int chanceOfRead) {
		this.chanceOfRead = chanceOfRead;
	}

	/**
	 * @return the chanceOfWrite
	 */
	public int getChanceOfWrite() {
		return chanceOfWrite;
	}

	/**
	 * @param chanceOfWrite the chanceOfWrite to set
	 */
	public void setChanceOfWrite(int chanceOfWrite) {
		this.chanceOfWrite = chanceOfWrite;
	}

	/**
	 * @return the chanceOfReadModifyWrite
	 */
	public int getChanceOfReadModifyWrite() {
		return chanceOfReadModifyWrite;
	}

	/**
	 * @param chanceOfReadModifyWrite the chanceOfReadModifyWrite to set
	 */
	public void setChanceOfReadModifyWrite(int chanceOfReadModifyWrite) {
		this.chanceOfReadModifyWrite = chanceOfReadModifyWrite;
	}

	/**
	 * @return the minTransactionSize
	 */
	public int getMinTransactionSize() {
		return minTransactionSize;
	}

	/**
	 * @param minTransactionSize the minTransactionSize to set
	 */
	public void setMinTransactionSize(int minTransactionSize) {
		this.minTransactionSize = minTransactionSize;
	}

	/**
	 * @return the maxTransactionSize
	 */
	public int getMaxTransactionSize() {
		return maxTransactionSize;
	}

	/**
	 * @param maxTransactionSize the maxTransactionSize to set
	 */
	public void setMaxTransactionSize(int maxTransactionSize) {
		this.maxTransactionSize = maxTransactionSize;
	}


}
