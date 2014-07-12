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

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import org.jboss.sdb.nosqltest.dbkeygen.KeyStore;
import org.jboss.sdb.nosqltest.dbmachines.DBMachine;
import org.jboss.sdb.nosqltest.dbmachines.FoundationDBASyncNoRetry;
import org.jboss.sdb.nosqltest.dbmachines.FoundationDBBlockingNoRetry;
import org.jboss.sdb.nosqltest.dbmachines.FoundationDB;
import org.jboss.sdb.nosqltest.dbmachines.TokuMXOptimist;
import org.jboss.sdb.nosqltest.dbmachines.TokuMXPessimist;
import org.jboss.sdb.nosqltest.dbmachines.TokuMX;


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
	final public static int FDB_BLOCK_NO_RETRY=11;
	final public static int FDB_ASYNC_NO_RETRY=12;
	final public static int FDB_COMPENSATION =13;
	
	final public static int MONGODB = 20;
	final public static int MONGODB_COMPENSATION = 20;
	
	final public static int TOKUMX = 30;
	final public static int TOKUMX_ACID_OC = 31;
	final public static int TOKUMX_ACID_PC = 32;
	final public static int TOKUMX_COMPEN = 33;
	
	private int dbType;
	
	private int chanceOfRead; 
	private int chanceOfWrite; 
	private int chanceOfReadModifyWrite; 
	private int chanceOfBalanceTransfer;
	private int chanceOfIncrementalUpdate;
	
	private int millisBetweenActions;
	
	private int minTransactionSize;
	private int maxTransactionSize;
	
	private KeyStore keys; 
	
	private int writeToLogs;
	
	DBWorker(	int dbType, 
				int chanceOfRead, 
				int chanceOfWrite, 
				int chanceOfReadModifyWrite,
				int chanceOfBalanceTransfer,
				int chanceOfIncrementalUpdate,
				int minTransactionSize,
				int maxTransactionSize,
				int millisBetweenActions,
				KeyStore availibleKeys,
				int writeToLogs){
		
		//Set the Parameters
		this.dbType = dbType;
		
		this.chanceOfRead = chanceOfRead; 
		this.chanceOfWrite = chanceOfWrite; 
		this.chanceOfReadModifyWrite = chanceOfReadModifyWrite; 
		this.chanceOfBalanceTransfer = chanceOfBalanceTransfer;
		this.chanceOfIncrementalUpdate = chanceOfIncrementalUpdate;
		
		
		this.millisBetweenActions = millisBetweenActions;
		this.minTransactionSize = minTransactionSize;
		this.maxTransactionSize = maxTransactionSize;
		this.keys = availibleKeys;
		this.writeToLogs = writeToLogs;
		
		
		//Set up the relevant Database Machine
		if (dbType == FDB){
			machine = new FoundationDB();
		}else if (dbType == FDB_BLOCK_NO_RETRY){
			//machine = new FDBBlockingNoRetry();
		}else if (dbType == FDB_ASYNC_NO_RETRY){
			//machine = new FDBASyncNoRetry();
		}else if (dbType == MONGODB){
			//machine = new MongoDBMachine();
		}else if (dbType == TOKUMX){
			machine = new TokuMX();
		}else if (dbType == TOKUMX_ACID_OC)	{
			machine = new TokuMXOptimist();
		}else if (dbType == TOKUMX_ACID_PC)	{
			machine = new TokuMXPessimist();
		}
					
		
		//Connect the DB
		machine.connectDB();
		
		//Set up some log tables 
		for (int i = 0; i > writeToLogs; i++){
			machine.addTable("Log"+i);
		}			
		
		if (chanceOfBalanceTransfer > 0 &&(this.minTransactionSize == 2||this.maxTransactionSize == 2)){
			//System.out.println("Balance Transfer may occur, setting transaction size to 2.");
			this.minTransactionSize = 2;
			this.maxTransactionSize = 2;
		}

	}
	
	public T doWork(T context, int niters, Result<T> opts) {
    	
		opts.setErrorCount(0);

		
		ActionRecord record = new ActionRecord();
		ActionRecord logRecord = new ActionRecord();
		
		final int transactionSize = ThreadLocalRandom.current().nextInt(this.maxTransactionSize)+this.minTransactionSize; 
		List<String> keysToUse = keys.getRandomKeyList(transactionSize, true);  
		
		
		//If this is a logger 
		if(writeToLogs > 0 && ThreadLocalRandom.current() .nextInt(1000)<1){
			System.out.println("Log Reader");
			
			record = machine.readLog(3,millisBetweenActions);
			workTimeMillis = System.currentTimeMillis();
			return null;
		}
		
		if (keysToUse.size()<2){
			System.out.println("whoa there...");
		}
		
		
    	//Get Random number to assign task
    	final int rand1 = ThreadLocalRandom.current() .nextInt(1000);
    	if (rand1< chanceOfRead){
    		//Reader
    		record = machine.read(keysToUse,millisBetweenActions);
    	}else if(rand1 < chanceOfWrite){
    		//Writer
    		record = machine.update(keysToUse, millisBetweenActions);
    	}else if(rand1 < chanceOfReadModifyWrite){
    		//Reader + Writer
    		record = machine.readModifyWrite(keysToUse, millisBetweenActions);
    	}else if (rand1 < chanceOfBalanceTransfer){
    		record = machine.balanceTransfer(keysToUse.get(0), keysToUse.get(1), millisBetweenActions);
    	}else if (rand1 < chanceOfIncrementalUpdate){
    		record = machine.incrementalUpdate(keysToUse, millisBetweenActions);
    	}
    	
    	
    	//Write the logs (if there are any to write)
    	if (writeToLogs>0)
    		logRecord = machine.writeLog(writeToLogs, millisBetweenActions);
    	
    	
    	//Check for success
    	if ( record ==null || !record.isSuccess() || 
    			(writeToLogs >0 && (logRecord ==null || !logRecord.isSuccess()) )){
    		
    		opts.incrementErrorCount();
    	}
    		

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
