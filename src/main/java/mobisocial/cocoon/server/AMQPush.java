package mobisocial.cocoon.server;

import gnu.trove.map.hash.TObjectIntHashMap;

import java.io.IOException;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

import javapns.Push;
import javapns.devices.Device;
import javapns.devices.exceptions.InvalidDeviceTokenFormatException;
import javapns.notification.PushNotificationPayload;
import javapns.notification.PushedNotification;
import javapns.notification.PushedNotifications;
import javapns.notification.ResponsePacket;
import javapns.notification.transmission.PushQueue;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;

import mobisocial.cocoon.model.Listener;
import mobisocial.cocoon.util.Database;
import net.vz.mongodb.jackson.JacksonDBCollection;

import org.apache.commons.codec.binary.Base64;

import com.mongodb.DBCollection;
import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.AMQP.Queue.DeclareOk;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.sun.jersey.spi.resource.Singleton;

@Singleton
@Path("/api/0/")
public class AMQPush {
	
	TObjectIntHashMap<String> mCounts = new TObjectIntHashMap<String>();
	HashMap<String, String> mQueues = new HashMap<String, String>();
	HashMap<String, String> mConsumers = new HashMap<String, String>();
	HashMap<String, HashSet<String>> mNotifiers = new HashMap<String, HashSet<String>>();
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
			final PushQueue dev_queue = Push.queue("push.p12", "pusubi", false, 1);
			dev_queue.start();
			final PushQueue prod_queue = Push.queue("pushprod.p12", "pusubi", false, 1);
			prod_queue.start();

	        
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
						String identity = mConsumers.get(consumerTag);
						if(identity == null)
							return;
						HashSet<String> devices = mNotifiers.get(identity);
						if(devices == null)
							return;
						threadDevices.addAll(devices);
					}
				    for(String device : threadDevices) {
						try {
							int new_value = 0;
							boolean production = false;
							synchronized (mNotifiers) {
								Listener l = mListeners.get(device);
								production = l.production != null && l.production != false;
							}
							synchronized (mCounts) {
								new_value = mCounts.adjustOrPutValue(device, 1, 1);
							}
							PushNotificationPayload payload = PushNotificationPayload.badge(new_value);
							if(!production)
								dev_queue.add(payload, device);
							else
								prod_queue.add(payload, device);
						} catch (InvalidDeviceTokenFormatException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}
			};
			
			System.out.println("doing registrations");
			Set<String> notifiers = new HashSet<String>();
			synchronized(mNotifiers) {
				notifiers.addAll(mNotifiers.keySet());
			}
			for(String identity : notifiers) {
				DeclareOk x = mIncomingChannel.queueDeclare();
				System.out.println("listening " + identity);
				mIncomingChannel.exchangeDeclare(identity, "fanout", true);
				mIncomingChannel.queueBind(x.getQueue(), identity, "");
				String consumerTag = mIncomingChannel.basicConsume(x.getQueue(), true, mConsumer);
				synchronized(mNotifiers) {
					mQueues.put(identity, x.getQueue());
					mConsumers.put(consumerTag, identity);
				}
			}
			System.out.println("done registrations");
			
			//TODO: don't do all the feedback stuff on one thread
			long last = new Date().getTime();
			for(;;) {
				Runnable job = mJobs.poll(60, TimeUnit.SECONDS);
				long current = new Date().getTime();
				if(current - last > 60 * 1000) {
					PushedNotifications ps = dev_queue.getPushedNotifications();
					for(PushedNotification p : ps) {
						if(p.isSuccessful())
							continue;
                        String invalidToken = p.getDevice().getToken();
                        System.err.println("unregistering invalid token " + invalidToken);
                        unregister(invalidToken);

                        /* Find out more about what the problem was */  
                        Exception theProblem = p.getException();
                        theProblem.printStackTrace();

                        /* If the problem was an error-response packet returned by Apple, get it */  
                        ResponsePacket theErrorResponse = p.getResponse();
                        if (theErrorResponse != null) {
                                System.out.println(theErrorResponse.getMessage());
                        }					
                    }
					last = new Date().getTime();

					List<Device> inactiveDevices = Push.feedback("push.p12", "pusubi", false);
	                for(Device d : inactiveDevices) {
                        String invalidToken = d.getToken();
                        System.err.println("unregistering feedback failed token token " + invalidToken);
	                	unregister(invalidToken);
	                }
				}
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
        	for(String ident : l.identityExchanges) {
        		HashSet<String> listeners = mNotifiers.get(ident);
        		if(listeners == null) {
        			listeners = new HashSet<String>();
        			mNotifiers.put(ident, listeners);
        		}
        		listeners.add(l.deviceToken);
        	}
        }
	}

	@POST
    @Path("register")
    @Produces("application/json")
    public String register(Listener l) throws IOException {
		boolean needs_update = true;
        synchronized(mNotifiers) {
        	System.out.println( new Date() + "Registering device: " + l.deviceToken + " for identities " + Arrays.toString(l.identityExchanges.toArray()));
        	
        	//clear pending message count on registration (e.g. amqp connected to drain messages)
        	//TODO: this is not really right if the client fails to download all messages
        	//before disconnecting
			synchronized (mCounts) {
				mCounts.put(l.deviceToken, 0);
			}
        	Listener existing = mListeners.get(l.deviceToken);
        	if(existing != null && existing.production == l.production && existing.identityExchanges.size() == l.identityExchanges.size()) { 
        		needs_update = false;
        		Iterator<String> a = existing.identityExchanges.iterator();
        		Iterator<String> b = l.identityExchanges.iterator();
        		while(a.hasNext()) {
        			String aa = a.next();
        			String bb = b.next();
        			if(!aa.equals(bb)) {
        				needs_update = true;
        				break;
        			}
        		}
        	}
        	if(!needs_update)
        		return "ok";
        	
        	mListeners.put(l.deviceToken, l);
        	
        	//TODO: set intersection to not wasteful tear up and down

        	if(existing != null) {
	        	//remove all old registrations
	        	for(String ident : existing.identityExchanges) {
	        		HashSet<String> listeners = mNotifiers.get(ident);
	        		assert(listeners != null);
	        		listeners.remove(l.deviceToken);
	        		if(listeners.size() == 0) {
	        			amqpUnregister(ident);
	        			mNotifiers.remove(ident);
	        		}
	        	}
        	}
        	
        	//add all new registrations
        	for(String ident : l.identityExchanges) {
        		HashSet<String> listeners = mNotifiers.get(ident);
        		if(listeners == null) {
        			listeners = new HashSet<String>();
        			mNotifiers.put(ident, listeners);
        			amqpRegister(ident);
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
	@POST
    @Path("clearunread")
    @Produces("application/json")
    public String clearUnread(String deviceToken) throws IOException {
        synchronized(mNotifiers) {
			synchronized (mCounts) {
				mCounts.put(deviceToken, 0);
			}
        }
        return "ok";
    }
	void amqpRegister(final String identity) {
		mJobs.add(new Runnable() {
			@Override
			public void run() {
				try {
					DeclareOk x = mPushThread.mIncomingChannel.queueDeclare();
					System.out.println("listening " + identity);
					mPushThread.mIncomingChannel.exchangeDeclare(identity, "fanout", true);
					mPushThread.mIncomingChannel.queueBind(x.getQueue(), identity, "");
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
	void amqpUnregister(final String identity) {
		mJobs.add(new Runnable() {
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
				System.out.println("stop listening " + identity);
				try {
					mPushThread.mIncomingChannel.queueUnbind(queue, identity, "");
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
        	synchronized (mCounts) {
        		mListeners.remove(deviceToken);
			}

        	//remove all old registrations
        	for(String ident : existing.identityExchanges) {
        		HashSet<String> listeners = mNotifiers.get(ident);
        		assert(listeners != null);
        		listeners.remove(deviceToken);
        		if(listeners.size() == 0) {
        			amqpUnregister(ident);
        			mNotifiers.remove(ident);
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
