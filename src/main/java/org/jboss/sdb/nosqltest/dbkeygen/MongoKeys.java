package org.jboss.sdb.nosqltest.dbkeygen;

import org.jboss.sdb.nosqltest.dbmachines.DBMachine;
import org.jboss.sdb.nosqltest.dbmachines.TokuMX;


public class MongoKeys extends StandardKeyStore{
	
	public void addKeysFromDB(int numberOfkeys, int dbType) {
		
		//Set up a standard Toku dbMachine and get some keys
		DBMachine machine = new TokuMX();
					
		//Connect the DB
		machine.connectDB();
		
		//initiate the keys 
		initKeys(machine.getKeysFromDB(numberOfkeys));
		
	}

}
