package com.winbold.storm;

import java.util.Map;
import java.util.Random;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class EndNode2Sensor1 extends BaseRichSpout 
{
    private SpoutOutputCollector collector;
    
    private static int currentNumber = 1;
        
    public void open( Map conf, TopologyContext context, SpoutOutputCollector collector ) 
    {
        this.collector = collector;
    }
    
    public void nextTuple() 
    {
    	Random r = new Random();

        String alphabet = "zxcvbnmlkjhgfdsaqwertyuiop";
        int randomNumber = r.nextInt(alphabet.length()-2);
        
        collector.emit( new Values( new String(alphabet.substring(randomNumber, randomNumber+1)) ) );
    }

    @Override
    public void ack(Object id) 
    {
    }

    @Override
    public void fail(Object id) 
    {
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) 
    {
        declarer.declare( new Fields( "letter" ) );
    }
}