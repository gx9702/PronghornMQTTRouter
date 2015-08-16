package com.ociweb.gateway.broker;

import com.ociweb.pronghorn.ring.RingBuffer;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class RouteSubscribeStage extends PronghornStage {

    protected RouteSubscribeStage(GraphManager graphManager, RingBuffer input, RingBuffer output) {
        super(graphManager, input, output);
        // TODO Auto-generated constructor stub
    }

    @Override
    public void run() {
        // TODO Auto-generated method stub
        
    }

}
