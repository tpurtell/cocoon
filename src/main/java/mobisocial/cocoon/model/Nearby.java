package mobisocial.cocoon.model;

import java.util.HashSet;

import net.vz.mongodb.jackson.Id;
import net.vz.mongodb.jackson.MongoCollection;

@MongoCollection(name = Nearby.COLLECTION)
public class Nearby {
	public static final String COLLECTION = "nearby";
	
	@Id 
	public String _id;
	
	public HashSet<String> buckets;
	public long expiration;
	public String data;
}
