package mobisocial.cocoon.server;

import gnu.trove.list.array.TByteArrayList;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.LinkedBlockingDeque;

import javapns.Push;
import javapns.communication.exceptions.CommunicationException;
import javapns.communication.exceptions.KeystoreException;
import javapns.devices.exceptions.InvalidDeviceTokenFormatException;
import javapns.notification.PushNotificationPayload;
import javapns.notification.transmission.PushQueue;

import javax.management.RuntimeErrorException;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;

import org.apache.commons.codec.binary.Base64;
import org.json.JSONException;

import mobisocial.cocoon.model.Listener;
import mobisocial.cocoon.util.Database;
import net.vz.mongodb.jackson.JacksonDBCollection;

import com.mongodb.DBCollection;
import com.rabbitmq.client.AMQP.Queue.DeclareOk;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.AMQP.BasicProperties;
import com.sun.jersey.spi.resource.Singleton;

@Singleton
@Path("/api/0/")
public class AMQPush {
	
	HashMap<TByteArrayList, String> mQueues = new HashMap<TByteArrayList, String>();
	HashMap<String, TByteArrayList> mConsumers = new HashMap<String, TByteArrayList>();
	HashMap<TByteArrayList, HashSet<String>> mNotifiers = new HashMap<TByteArrayList, HashSet<String>>();
	HashMap<String, Listener> mListeners = new HashMap<String, Listener>();
	LinkedBlockingDeque<Runnable> mJobs = new LinkedBlockingDeque<Runnable>();
	String encodeAMQPname(String prefix, byte[] key) {
		return prefix + Base64.encodeBase64(key, false, true) + "\n";
	}
    byte[] decodeAMQPname(String prefix, String name) {
    	if(!name.startsWith(prefix))
    		return null;
    	//URL-safe? automatically, no param necessary?
    	return Base64.decodeBase64(name.substring(prefix.length()));
	}
    
    AMQPushThread mPushThread = new AMQPushThread();
    
    class AMQPushThread extends Thread{
    	Channel mIncomingChannel;
		private DefaultConsumer mConsumer;
		@Override
		public void run() {
        	try {
				amqp();
			} catch (Throwable e) {
				throw new RuntimeException(e);
			}
		}
		void amqp() throws Throwable {
	        PushQueue appleQueue = null;
			appleQueue = Push.queue("push.p12", "pusubi", false, 2);
			appleQueue.start();
			final PushQueue queue = appleQueue;

	        
	        ConnectionFactory connectionFactory = new ConnectionFactory();
			connectionFactory.setHost("bumblebee.musubi.us");
			connectionFactory.setConnectionTimeout(30 * 1000);
			connectionFactory.setRequestedHeartbeat(30);
			Connection connection = connectionFactory.newConnection();
			mIncomingChannel = connection.createChannel();
			
			mConsumer = new DefaultConsumer(mIncomingChannel) {
				@Override
				public void handleDelivery(final String consumerTag, final Envelope envelope,
						final BasicProperties properties, final byte[] body) throws IOException 
				{
					HashSet<String> threadDevices = new HashSet<String>();
					synchronized (mNotifiers) {
						TByteArrayList identity = mConsumers.get(consumerTag);
						if(identity == null)
							return;
						HashSet<String> devices = mNotifiers.get(identity);
						if(devices == null)
							return;
						threadDevices.addAll(devices);
					}
					for(String device : threadDevices) {
						try {
							queue.add(new PushNotificationPayload("Musubis!!!"), device);
						} catch (InvalidDeviceTokenFormatException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						} catch (JSONException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}
			};
			
			System.out.println("doing registrations");
			Set<TByteArrayList> notifiers = new HashSet<TByteArrayList>();
			synchronized(mNotifiers) {
				notifiers.addAll(mNotifiers.keySet());
			}
			for(TByteArrayList identity : notifiers) {
				DeclareOk x = mIncomingChannel.queueDeclare();
				String identity_exchange_name = encodeAMQPname("ibeidentity-", identity.toArray());
				System.out.println("listening " + identity_exchange_name);
				mIncomingChannel.queueBind(x.getQueue(), identity_exchange_name, "");
				String consumerTag = mIncomingChannel.basicConsume(x.getQueue(), true, mConsumer);
				synchronized(mNotifiers) {
					mQueues.put(identity, x.getQueue());
					mConsumers.put(consumerTag, identity);
				}
			}
			System.out.println("done registrations");
			
			for(;;) {
				Runnable job = mJobs.poll();
				if(job == null)
					continue;
				job.run();
			}
		}
	};
    
	public AMQPush() {
		loadAll();
		mPushThread.start();
	}
	
    private void loadAll() {
        DBCollection rawCol = Database.dbInstance().getCollection(Listener.COLLECTION);
        JacksonDBCollection<Listener, String> col = JacksonDBCollection.wrap(rawCol,
        		Listener.class, String.class);

        for(Listener l : col.find()) {
        	mListeners.put(l.deviceToken, l);

        	//add all registrations
        	for(byte[] ident : l.identities) {
        		TByteArrayList identity = new TByteArrayList(ident);
        		HashSet<String> listeners = mNotifiers.get(identity);
        		if(listeners == null) {
        			listeners = new HashSet<String>();
        			mNotifiers.put(identity, listeners);
        		}
        		listeners.add(l.deviceToken);
        	}
        }
	}

	@POST
    @Path("register")
    @Produces("application/json")
    public String register(Listener l) throws IOException {
		boolean needs_update = false;
        synchronized(mNotifiers) {
        	Listener existing = mListeners.get(l.deviceToken);
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
        	
        	mListeners.put(l.deviceToken, l);

        	//remove all old registrations
        	for(byte[] ident : existing.identities) {
        		final TByteArrayList identity = new TByteArrayList(ident);
        		HashSet<String> listeners = mNotifiers.get(identity);
        		assert(listeners != null);
        		listeners.remove(l.deviceToken);
        		if(listeners.size() == 0) {
        			amqpUnregister(identity);
        			mNotifiers.remove(identity);
        		}
        	}

        	//add all new registrations
        	for(byte[] ident : l.identities) {
        		final TByteArrayList identity = new TByteArrayList(ident);
        		HashSet<String> listeners = mNotifiers.get(identity);
        		if(listeners == null) {
        			listeners = new HashSet<String>();
        			mNotifiers.put(identity, listeners);
        			amqpRegister(identity);
        		}
        		listeners.add(l.deviceToken);
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
	void amqpRegister(final TByteArrayList identity) {
		mJobs.push(new Runnable() {
			@Override
			public void run() {
				try {
					DeclareOk x = mPushThread.mIncomingChannel.queueDeclare();
					String identity_exchange_name = encodeAMQPname("ibeidentity-", identity.toArray());
					System.out.println("listening " + identity_exchange_name);
					mPushThread.mIncomingChannel.queueBind(x.getQueue(), identity_exchange_name, "");
					String consumerTag = mPushThread.mIncomingChannel.basicConsume(x.getQueue(), true, mPushThread.mConsumer);
					synchronized(mNotifiers) {
						mQueues.put(identity, x.getQueue());
						mConsumers.put(consumerTag, identity);
					}
				} catch (Throwable t) {
					throw new RuntimeException("failed to register", t);
				}
			}
		});
	}
	void amqpUnregister(final TByteArrayList identity) {
		mJobs.push(new Runnable() {
			@Override
			public void run() {
				String queue = null;
				synchronized(mNotifiers) {
					queue = mQueues.get(identity);
					//probably an error
					if(queue == null)
						return;
					mQueues.remove(identity);
					//TODO: update consumers
				}
				String identity_exchange_name = encodeAMQPname("ibeidentity-", identity.toArray());
				System.out.println("stop listening " + identity_exchange_name);
				try {
					mPushThread.mIncomingChannel.queueUnbind(queue, encodeAMQPname("ibeidentity-", identity.toArray()), "");
				} catch (Throwable t) {
					throw new RuntimeException("removing queue dynamically", t);
				}
			}
		});
	}

    @POST
    @Path("unregister")
    @Produces("application/json")
    public String unregister(String deviceToken) throws IOException {
        synchronized(mNotifiers) {
        	Listener existing = mListeners.get(deviceToken);
        	if(existing == null)
        		return "ok";
        	
        	mListeners.remove(deviceToken);

        	//remove all old registrations
        	for(byte[] ident : existing.identities) {
        		TByteArrayList identity = new TByteArrayList(ident);
        		HashSet<String> listeners = mNotifiers.get(identity);
        		assert(listeners != null);
        		listeners.remove(deviceToken);
        		if(listeners.size() == 0) {
        			amqpUnregister(identity);
        			mNotifiers.remove(identity);
        		}
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
