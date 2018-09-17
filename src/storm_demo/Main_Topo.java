package storm_demo;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;

public class Main_Topo {

	public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException, AuthorizationException {
		
		TopologyBuilder topo_Builder = new TopologyBuilder();
		topo_Builder.setSpout("phone_Spout", new Phone_Spout(),16);
		topo_Builder.setBolt("upper_Bolt", new Upper_Bolt()).shuffleGrouping("phone_Spout");
		topo_Builder.setBolt("suffix_Bolt", new Suffix_Bolt()).shuffleGrouping("upper_Bolt");
		
		StormTopology topo = topo_Builder.createTopology();
		
		Config conf = new Config();
		conf.setNumWorkers(16);
		
		StormSubmitter.submitTopology("Storm_Demo", conf, topo);
	}
	
}
