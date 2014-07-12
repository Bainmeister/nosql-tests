package org.jboss.sdb.nosqltest.dbkeygen;

import org.jboss.sdb.nosqltest.dbmachines.DBMachine;
import org.jboss.sdb.nosqltest.dbmachines.FoundationDB;

public class FDBKeys extends StandardKeyStore{

	public void addKeysFromDB(int numberOfkeys, int dbtype) {
		//Set up a standard Foundation dbMachine and get some keys
		DBMachine machine = new FoundationDB();
					
		//Connect the DB
		machine.connectDB();
		
		//initiate the keys 
		initKeys(machine.getKeysFromDB(numberOfkeys));
		
	}

}
