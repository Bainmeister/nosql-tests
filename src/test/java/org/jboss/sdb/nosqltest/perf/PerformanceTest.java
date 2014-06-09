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

import org.junit.Test;

import io.narayana.perf.PerformanceTester;
import io.narayana.perf.Result;

/**
 * @author <a href="mailto:s.bain@newcastle.ac.uk">Simon Bain</a>
 *
 * A Worker that interacts with DBMachine to manipulate Databases. 
 */
public class PerformanceTest {

    //***********************************************************
    //STANDARD PARAMETERS    
    final int threadCount = 10;
    final int batchSize = 1;
    final int numberOfCalls = 1000;
    
    
    //DB Parameters
    final int keyLength = 2;
    final int dbType = DBWorker.FDB;
    final int chanceOfRead = 0;
    final int chanceOfWrite = 0;
    final int chanceOfReadModifyWrite = 100;
    final int minTransactionSize = 1;
    final int maxTransactionSize = 1;   
    
    
    //TODO - Size of Read, Size of Write (Min/Max)
    //***********************************************************
	
    
    @Test
    public void testPerformanceTester() {
    	
        
        DBWorker<Void> worker = new DBWorker<Void>(keyLength, 
        								dbType, 
        								chanceOfRead, 
        								chanceOfWrite, 
        								chanceOfReadModifyWrite, 
        								minTransactionSize,
        								maxTransactionSize);
        
        //Run the test
        Result measurement = new Result(threadCount, numberOfCalls, batchSize).measure(worker, worker, 100);
        
        
        System.out.println("Threads: " + measurement.getThreadCount());
        System.out.println("Calls: " + measurement.getNumberOfCalls());
        System.out.println("Batch Size: " + measurement.getBatchSize());
        System.out.println("Thoroughput: " + measurement.getThroughput());
        System.out.println("millies: " + measurement.getTotalMillis());
        
        
    }
    
    
    
    /*
    @Test
    public void testPerformanceTester() {


        
        PerformanceTester<Void> tester = new PerformanceTester<Void>(10, batchSize);
        DBWorker<Void> worker = new DBWorker<Void>(keyLength, 
        								dbType, 
        								chanceOfRead, 
        								chanceOfWrite, 
        								chanceOfReadModifyWrite, 
        								minTransactionSize,
        								maxTransactionSize);
        
        //Set the options
        Result<Void> opts = new Result<Void>(threadCount, numberOfCalls);
        
        
        try {
        	
        	//tester.measureThroughput(lifecycle, workload, callCount, threadCount, batchSize, warmUpCallCount)
        	Result<Void> res = tester.measureThroughput(worker, opts);
            long start = System.nanoTime();
            long millis = (System.nanoTime() - start) / 1000000L;

           
            System.out.printf("Throughput "+ res.getThroughput());
            
            //System.out.printf("Test performance for %d!: %d calls / second (total time: %d ms versus %d ms)%n",
            //        opts.getNumberOfCalls(), opts.getThroughput(), opts.getTotalMillis(), millis);

        } finally {
            tester.fini();
        }
    }
	*/
    
    
}
