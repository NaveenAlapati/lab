package com.winbold.storm;


import java.util.Map;

import com.winbold.storm.util.Constants;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class EMSBolt extends BaseRichBolt 
{
    private OutputCollector collector;
    private String recentPressureMonitorData = "";
    private int recentTemperatureMonitorData = 0;
    private String temp = "mnbvcxzasdfgh";
    private int temperatureCount = 0;
    private int pressureCount = 0;
    
	StringBuilder temperatureMonitorData = new StringBuilder();
	StringBuilder pressureMonitorData = new StringBuilder();

    public void prepare( Map conf, TopologyContext context, OutputCollector collector ) 
    {
        this.collector = collector;
    }

    public void execute( Tuple tuple ) 
    { 
        //System.out.println("###############EMSBolt****EMSBolt*****EMSBolt#############");
        try{
        	String finaldata = tuple.getSourceComponent();
        	System.out.println(tuple.getValue(0).toString());
          System.out.println("-------------------------");
        	//add the recent temperature into a temp variable
        	if(tuple.getSourceComponent().equals(Constants.TEMPERATURE_MONITOR)){
        		recentTemperatureMonitorData = tuple.getInteger(0);
        		temperatureCount++;
        		System.out.println("Temparature Count: "+temperatureCount);
        	}
        	//add recent pressure into a temp variable
        	else{
        		recentPressureMonitorData = tuple.getString(0);
        		pressureCount++;
        		System.out.println("Pressure Count: "+pressureCount);
        	}
        	//send an alert if the temperature is less than 55 and the input letter is not there in temp variable
        	if(!temp.contains(recentPressureMonitorData) && recentTemperatureMonitorData <=30 ){
        		System.out.println("******************Alert: Pressure is Low..");
        	}
        	else if(temp.contains(recentPressureMonitorData) && recentTemperatureMonitorData >=30){
        		System.out.println("******************Alert: Temperature is High..");
        	}
        	
        	
        	
        }catch(Exception e){
        	System.out.println("Exception in reading values");
        }
        
        collector.emit(new Values( new String(tuple.getValue(0).toString()) ));
        collector.ack(tuple);
    }

    public void declareOutputFields( OutputFieldsDeclarer declarer ) 
    {
        declarer.declare( new Fields( "number" ) );
    }   
}