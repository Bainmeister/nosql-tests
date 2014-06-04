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

import org.jboss.sdb.nosqltest.perf.ActionRecord;

/**
 * @author <a href="mailto:s.bain@newcastle.ac.uk">S Bain</a>
 *
 * Interface for DB interaction tests.
 */
public interface DBMachine {
    
	/**
	 * Connect to the Database
	 */
	void connectDB();
	
	/**
	 * Disconnect from the Database
	 */
	void disconnectDB();
	
	/**
	 * Performs (transactionSize) reads against the database that is currently connected.
	 *  
	 * @param  keyLength The length of a key - "00" = 2, "000" = 3, "0000" = 4
	 * @param  transActionSize The number of actions within the transaction
	 * @return the attempts taken to complete the transaction
	 */
	ActionRecord read(int keyLength, int transactionSize);
		
	/**
	 * Performs (transactionSize) updates to the database that is currently connected.
	 *  
	 * @param  keyLength The length of a key - "00" = 2, "000" = 3, "0000" = 4
	 * @param  transActionSize The number of actions within the transaction
	 * @return the attempts taken to complete the transaction
	 */
	ActionRecord write(int keyLength, int transactionSize);
	
	/**
	 * Performs (transactionSize) read and write operations to the database that is 
	 * currently connected.
	 *  
	 * @param  keyLength The length of a key - "00" = 2, "000" = 3, "0000" = 4
	 * @param  transActionSize The number of actions within the transaction
	 * @return the attempts taken to complete the transaction
	 */
	ActionRecord readModifyWrite(int keyLength, int transactionSize);
	
}
