package org.jboss.sdb.nosqltest.dbkeygen;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class StandardKeyStore implements KeyStore{

	private HashMap<String, String> keys = new HashMap<String, String>();	
	
	public void addKeysFromDB(int numberOfkeys, int dbType) {
		// TODO Auto-generated method stub
		
	}

	public void addKey(String key) {
		keys.put(key, "");
	}
	
	public String getKey(int rand){
		return new ArrayList<String>(keys.keySet()).get(rand);
	}

	public void initKeys(HashMap<String, String> keys){
		this.keys = new HashMap<String, String>(keys);
	}
	
	public void removeKey(String key) {
		keys.remove(key);
	}

	public void removeAllKeys() {
		keys.clear();
	}

	public int getNumberOfKeys() {
		return keys.size();
	}

	public HashMap<?, ?> getAllKeys() {
		return new HashMap<String, String>(keys);
	}

	public List<String> getRandomKeyList(int numberToGet,
			boolean allowDuplicates) {
		
		int keyNum = getNumberOfKeys();
		
		if (numberToGet > keyNum && allowDuplicates ==false){
			
			//In this case, we will not be able to finish, don't stop - just increase
			// the size of number to get to match
			System.out.println("Decreasing transaction size to match availible the number"+
								" of keys...");
			
			numberToGet = keyNum;
		}
		
		List<String> keysToReturn = new ArrayList<String>();
		
		for (int i = 0; i < numberToGet; i++){
			if (keyNum>0){
				
				//Add a random key from the set to list of keys to return
				final int rand = ThreadLocalRandom.current() .nextInt(keyNum);
				keysToReturn.add(getKey(rand));
			}
		}

		return keysToReturn;
	}

}
