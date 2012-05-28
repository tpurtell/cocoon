package mobisocial.cocoon.model;

import java.util.HashSet;

import net.vz.mongodb.jackson.Id;
import net.vz.mongodb.jackson.MongoCollection;

/**
 * A device that has been registered for notifications for a set of identities.
 */
@MongoCollection(name = Listener.COLLECTION)
public class Listener {
	public static final String COLLECTION = "device";
	
	@Id 
	public String _id;
	
	public HashSet<String> identityExchanges;
	public String deviceToken;
	public Boolean production;
	public Integer localUnread;
}
