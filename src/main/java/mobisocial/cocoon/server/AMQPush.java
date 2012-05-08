package mobisocial.cocoon.server;

import gnu.trove.list.array.TByteArrayList;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

import javapns.Push;
import javapns.communication.exceptions.CommunicationException;
import javapns.communication.exceptions.KeystoreException;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;

import mobisocial.cocoon.model.Listener;
import mobisocial.cocoon.util.Database;
import net.vz.mongodb.jackson.JacksonDBCollection;

import com.mongodb.DBCollection;
import com.sun.jersey.spi.resource.Singleton;

@Singleton
@Path("/api/0/")
public class AMQPush {
	
	HashMap<TByteArrayList, HashSet<TByteArrayList>> mNotifiers = new HashMap<TByteArrayList, HashSet<TByteArrayList>>();
	HashMap<TByteArrayList, Listener> mListeners = new HashMap<TByteArrayList, Listener>();
	
	public AMQPush() {
		//TODO:create AMQP thread
		//TODO:load all notifiers from data base
		//TODO:set apple push queue thread(s)
		//TODO:add feedback thread (to call getPushedNotifications)
		

		try {
			Push.alert("Hello World!", "keystore.p12", "keystore_password", false, "Your token");
		} catch (CommunicationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (KeystoreException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
    @POST
    @Path("register")
    @Produces("application/json")
    public String register(Listener l) throws IOException {
        TByteArrayList deviceToken = new TByteArrayList(l.deviceToken);
		boolean needs_update = false;
        synchronized(mNotifiers) {
        	Listener existing = mListeners.get(deviceToken);
        	if(existing != null && existing.identities.size() == l.identities.size()) { 
        		Iterator<byte[]> a = existing.identities.iterator();
        		Iterator<byte[]> b = l.identities.iterator();
        		while(a.hasNext()) {
        			byte[] aa = a.next();
        			byte[] bb = b.next();
        			if(!Arrays.equals(aa, bb)) {
        				needs_update = true;
        				break;
        			}
        		}
        	}
        	if(!needs_update)
        		return "ok";
        	
        	mListeners.put(deviceToken, l);

        	//remove all old registrations
        	for(byte[] ident : existing.identities) {
        		TByteArrayList identity = new TByteArrayList(ident);
        		HashSet<TByteArrayList> listeners = mNotifiers.get(identity);
        		assert(listeners != null);
        		listeners.remove(deviceToken);
        	}

        	//add all new registrations
        	for(byte[] ident : l.identities) {
        		TByteArrayList identity = new TByteArrayList(ident);
        		HashSet<TByteArrayList> listeners = mNotifiers.get(identity);
        		if(listeners == null) {
        			listeners = new HashSet<TByteArrayList>();
        			mNotifiers.put(identity, listeners);
        		}
        		listeners.add(deviceToken);
        	}
        }
        DBCollection rawCol = Database.dbInstance().getCollection(Listener.COLLECTION);
        JacksonDBCollection<Listener, String> col = JacksonDBCollection.wrap(rawCol,
        		Listener.class, String.class);
        Listener match = new Listener();
        match.deviceToken = l.deviceToken;
        col.update(match, l, true, false);
        return "ok";
    }

    @POST
    @Path("unregister")
    @Produces("application/json")
    public String unregister(byte[] deviceToken) throws IOException {
        synchronized(mNotifiers) {
        	Listener existing = mListeners.get(deviceToken);
        	if(existing == null)
        		return "ok";
        	
        	mListeners.remove(deviceToken);

        	//remove all old registrations
        	for(byte[] ident : existing.identities) {
        		TByteArrayList identity = new TByteArrayList(ident);
        		HashSet<TByteArrayList> listeners = mNotifiers.get(identity);
        		assert(listeners != null);
        		listeners.remove(deviceToken);
        		if(listeners.size() == 0);
        		mNotifiers.remove(identity);
        	}
        }
        DBCollection rawCol = Database.dbInstance().getCollection(Listener.COLLECTION);
        JacksonDBCollection<Listener, String> col = JacksonDBCollection.wrap(rawCol,
        		Listener.class, String.class);
        Listener match = new Listener();
        match.deviceToken = deviceToken;
        col.remove(match);
        return "ok";
    }
}
