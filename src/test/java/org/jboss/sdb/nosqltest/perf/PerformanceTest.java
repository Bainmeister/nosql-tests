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

import org.jboss.sdb.nosqltest.dbkeygen.KeyStore;
import org.jboss.sdb.nosqltest.dbkeygen.MongoKeys;
import org.junit.Test;

import io.narayana.perf.Result;

/**
 * @author <a href="mailto:s.bain@newcastle.ac.uk">Simon Bain</a>
 *
 * A Worker that interacts with DBMachine to manipulate Databases. 
 */
public class PerformanceTest {

    //***********************************************************
    int threadCount = 10;
    int batchSize = 1;
    int numberOfCalls = 10000;
    //int keyLength = 2;
    int dbType = DBWorker.TOKUMX_ACID_OC;
   
    int chanceOfRead = 1;
    int chanceOfWrite =0;
    int chanceOfBalanceTransfer = 999;

    int chanceOfReadModifyWrite = 0;
    int chanceOfIncrementalUpdate =0; 
    
    int minTransactionSize = 2;
    int maxTransactionSize = 2; 
    
    int millisBetweenActions = 10;	
    int contendedRecords =2; 	//the more records, the lower the contention
    
    int writeToLogs = 0;
    //***********************************************************
    
    @Test
    public void testPerformanceTester() {
    	
    	System.out.println("Begin");
    	for (int i =0; i<10;i++){
    	
    	//Set up some keys to use.  This allows us to identify x rows that exist and are ready for use and
    	// allows us to avoid touching the db unnecessarily during the tests. 
    	//TODO implement other stores
    	//System.out.println("Generating Keys...");
    	KeyStore keyGen = new MongoKeys();   
		keyGen.addKeysFromDB(contendedRecords,dbType);
		
		//Setup the template worker
		//System.out.println("Defining worker template...");
		DBWorker<Void> worker = new DBWorker<Void>(	dbType, 
													chanceOfRead,	
													chanceOfWrite, 
													chanceOfReadModifyWrite, 
													chanceOfBalanceTransfer,
													chanceOfIncrementalUpdate,
													minTransactionSize, 
													maxTransactionSize, 
													millisBetweenActions,
													keyGen, writeToLogs);
	    
	    
	    //Run the test
		//System.out.println("Running Tests...");
	    Result<Void> measurement = new Result<Void>(threadCount, numberOfCalls, batchSize).measure(worker, worker, 100);
		
	    //Do something with the results
		//System.out.println("Tests complete.");
	    System.out.println(measurement);
    	}
    	System.out.println("End");
    }
    
    
    
 
}
