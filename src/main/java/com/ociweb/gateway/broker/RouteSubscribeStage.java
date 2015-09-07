package com.ociweb.gateway.broker;

import com.hazelcast.config.Config;
import com.hazelcast.config.ReliableTopicConfig;
import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Message;
import com.hazelcast.core.MessageListener;
import com.hazelcast.topic.ReliableMessageListener;
import com.hazelcast.topic.TopicOverloadException;
import com.hazelcast.topic.TopicOverloadPolicy;
import com.hazelcast.topic.impl.reliable.ReliableTopicProxy;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class RouteSubscribeStage extends PronghornStage {

    private HazelcastInstance instance;
    
    protected RouteSubscribeStage(GraphManager graphManager, Pipe input, Pipe output) {
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
        
    }

    @Override
    public void run() {
        
        //as new subscriber check here first for retained messages
        // retainedMessages //TODO: add this feature last after we fix the routing.
        
        
        //take incomming messages and publish them on topics.
        //if topic is new must pull new PathIDs
        //if topic is retained must set retained value
        
        String topicName = "test";
        
        ReliableTopicProxy topic = (ReliableTopicProxy)instance.getReliableTopic(topicName);

        topic.addMessageListener(new ReliableMessageListener() {

            @Override
            public void onMessage(Message message) {
                // TODO Auto-generated method stub
                
            }

            @Override
            public long retrieveInitialSequence() {
                // TODO Auto-generated method stub
                return 0;
            }

            @Override
            public void storeSequence(long sequence) {
                // TODO Auto-generated method stub
                
            }

            @Override
            public boolean isLossTolerant() {
                // TODO Auto-generated method stub
                return false;
            }

            @Override
            public boolean isTerminal(Throwable failure) {
                // TODO Auto-generated method stub
                return false;
            }
        });
        
     
    }

}
