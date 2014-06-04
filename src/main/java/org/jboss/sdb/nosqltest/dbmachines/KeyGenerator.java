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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * @author <a href="mailto:s.bain@newcastle.ac.uk">Simon Bain</a>
 *
 * Class contains functions for creating and storing random list and chances.
 *  Intended use is with the DBMachine (or similar). 
 */
public class KeyGenerator {
	
	/**
	 * Creates a thread safe list of randomKeys
	 * @param numberOfkeys
	 * @param keyWidth
	 * @return keyList
	 */
	public static List<String> randomKeys(int numberOfkeys, int keyWidth) {
    	
    	List<String> keyList = new ArrayList<String>();
    	
    	for (int i = 0 ; i<numberOfkeys; i++){
    		keyList.add(randomKey(keyWidth));
    	}
    	
		return keyList;
	}
	
	/**
	 * Create a thread safe random key of keyLength units
	 * @param keyLength
	 * @return
	 */
	public static String randomKey(int keyWidth) {
		
		String k ="";
		for (int i = 0 ; i < keyWidth; i++)
			k.concat(String.valueOf(ThreadLocalRandom.current() .nextInt(9)));
		
		return k;
	}
	
    /**
     * Creates a thread safe list of random ints
     * @param transactionSize
     * @return
     */
	public static List<Integer> RandomInts(int numberToCreate, int maxVal) {
    	
		List<Integer> randList = new ArrayList<Integer>();
    	
    	for (int i = 0 ; i<numberToCreate; i++){
    		randList.add(ThreadLocalRandom.current() .nextInt(maxVal));
    	}
    	
		return randList;
	}

}
