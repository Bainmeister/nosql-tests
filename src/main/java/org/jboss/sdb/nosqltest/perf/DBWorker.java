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


import io.narayana.perf.Worker;
import io.narayana.perf.Result;

import java.util.concurrent.ThreadLocalRandom;

import org.jboss.sdb.nosqltest.dbmachines.DBMachine;
import org.jboss.sdb.nosqltest.dbmachines.FDBStandard;


/**
 * @author <a href="mailto:s.bain@newcastle.ac.uk">Simon Bain</a>
 *
 * A Worker that interacts with DBMachine to manipulate Databases. 
 */
public class DBWorker implements Worker<Void>{
	
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
	
	DBWorker(int dbType){
		
		if (dbType == FDB){
			machine = new FDBStandard();
		}else if (dbType == FDB_NO_RETRY){
			//machine = new FDBNoRetry();
		}else if (dbType == FDB_COMPENSATION){
			//machine = new FDBCompensation();
		}else if (dbType == MONGODB){
			//machine = new MongoDBMachine();
		}		
		
		machine.connectDB();
	}
	
	public Void doWork(Void context, int niters, Result<Void> opts) {
    	
		//TODO
		//Get configuration
    	final int keyLength = 2;//opts.getKeyLength();
    	final int transactionSize = 10;//ThreadLocalRandom.current().nextInt(opts.getMaxTransactionSize())+1;  //Range 1 - Max rather than 0 to Max-1
    	    	
    	//Get Random number to assign task
    	final int rand1 = ThreadLocalRandom.current() .nextInt(100);
    	    	
    	ActionRecord record = new ActionRecord();
    	
    	//TODO fix this!
    	//Call the relevant method, depending upon ChanceOfProcess and the transactionSize
    	if (rand1< 100){
    		record = machine.read(keyLength, transactionSize);
    	}else if(rand1 < 0){
    		record = machine.write(keyLength, transactionSize);
    	}else{
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
}
