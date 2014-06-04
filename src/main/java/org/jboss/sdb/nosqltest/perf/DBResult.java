package org.jboss.sdb.nosqltest.perf;

import io.narayana.perf.Result;

public class DBResult<T> extends Result{

	public DBResult(int threadCount, int numberOfCalls) {
		super(threadCount, numberOfCalls);
		// TODO Auto-generated constructor stub
	}
	
    public DBResult(DBResult result) {
    	
    	super(result);
    
        
    }
	

}

