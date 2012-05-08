package mobisocial.cocoon.util;

import com.mongodb.DB;
import com.mongodb.Mongo;

public class Database {
	private static Mongo sInstance = null;
	public static Mongo instance() {
		synchronized(Database.class) {
			if(sInstance == null)
				try {
					sInstance = new Mongo();
				} catch (Exception e) {
					e.printStackTrace();
					System.exit(1);
				}
			return sInstance;
		}
	}
	public static DB dbInstance() {
		return instance().getDB("cocoon");
	}
}
