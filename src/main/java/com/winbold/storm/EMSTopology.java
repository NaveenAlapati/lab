package com.winbold.storm;

import com.winbold.storm.util.Constants;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

public class EMSTopology 
{
    public static void main(String[] args) 
    {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout( Constants.TEMPERATURE_SPOUT, new EndNode1Sensor1());
        builder.setSpout( Constants.PRESSURE_SPOUT, new EndNode2Sensor1());
        builder.setBolt( Constants.TEMPERATURE_MONITOR, new EndNode1Bolt1())
                .shuffleGrouping(Constants.TEMPERATURE_SPOUT);
        builder.setBolt( Constants.PRESSURE_MONITOR, new EndNode2Bolt1())
        .shuffleGrouping(Constants.PRESSURE_SPOUT);
        
        builder.setBolt( Constants.EMS, new EMSBolt())
        .shuffleGrouping(Constants.TEMPERATURE_MONITOR).shuffleGrouping(Constants.PRESSURE_MONITOR);
        


        Config conf = new Config();
        conf.setDebug(true);
        if (args != null && args.length > 0) {
            conf.setNumWorkers(3);
            

            try {
				StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
			} catch (AlreadyAliveException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InvalidTopologyException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
          }
        else {
            conf.setMaxTaskParallelism(3);
        
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("test", conf, builder.createTopology());
        Utils.sleep(100000);
        //cluster.killTopology("test");
        cluster.shutdown();
        }
    }
}