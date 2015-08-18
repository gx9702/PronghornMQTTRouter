package com.ociweb.gateway.broker;

import java.util.Map;

import com.hazelcast.config.Config;
import com.hazelcast.config.ReliableTopicConfig;
import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.topic.TopicOverloadException;
import com.hazelcast.topic.TopicOverloadPolicy;
import com.hazelcast.topic.impl.reliable.ReliableTopicProxy;
import com.ociweb.pronghorn.ring.RingBuffer;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;


/*
 * NEED: publsh to HZ and need subscrip to HZ as two diferent stages
 * 
 *  
 *  
 *  Map<Topic, Message> retained
 *  Map<Path, Children> subscriptions //Children can have other paths or subscriber ID
 *  Set<SubcriptionIDs>
 *  publish to subid,  and subecribe to subids 
 *  
 *  Children can be paths or subscriptionID.
 *  
 *  //publish:
 *  
 *    setup to publish on topic
 *       -- listen for updates to topic paths
 *       -- lookup the topic path subcribers and keep locally
 *    publish
 *       -- publish to subcriber ids   
 *  
 *  
 *  //subscribe:
 *       --check retained on new subscribe
 *       --subcribe to all parts
 *       
 *       
 *   /////////// alerternate approach
 *   
 *    
 *    
 *   
 * 
 * 
 */

public class RoutePublishStage extends PronghornStage {

    private static final String RETAINED_MESSAGES = "retainedMessages";
    private HazelcastInstance instance;
    private Map<byte[],byte[]> retainedMessages;

    protected RoutePublishStage(GraphManager graphManager, RingBuffer input, RingBuffer output) {
        super(graphManager, input, output);
        
        // TODO Auto-generated constructor stub
        
    }
   
    @Override
    public void startup() {
        Config config = new Config();

        //TODO: RESEARCH THE USE OF WILD CARD HERE AND WHAT IS SUPPORTED.
        config.addRingBufferConfig(new RingbufferConfig("*").setCapacity(100).setTimeToLiveSeconds(5));         
        config.addReliableTopicConfig(new ReliableTopicConfig("*").setTopicOverloadPolicy(TopicOverloadPolicy.ERROR));

        instance = Hazelcast.newHazelcastInstance(config);        
        
        retainedMessages = instance.getMap(RETAINED_MESSAGES);
        
    }
    
    @Override
    public void run() {
        
        //as publisher if messsage is retained write it here
        // retainedMessages  //TODO: add this feature last after we fix the routing.
        
        
        //take incomming messages and publish them on topics.
        //if topic is new must pull new PathIDs
        //if topic is retained must set retained value
        
        String topicName = "test";
        
        ReliableTopicProxy topic = (ReliableTopicProxy)instance.getReliableTopic(topicName);

        Object payload = null;
        
//
//        ringbufferService = getNodeEngineImpl(hz).getService(RingbufferService.SERVICE_NAME);
//        ringbufferContainer = ringbufferService.getContainer(ringbuffer.getName());
//        
        
        
        try {
            topic.publish(payload);  
        } catch (TopicOverloadException expected) {
            //TODO: unable to publish now the target ring is full 
            //      back off and schedule something else to be done try again next time this node is scheduled.
            
        }
        
        
    }

}
