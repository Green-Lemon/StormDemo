package storm_demo;

import java.util.Map;
import java.util.Random;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class Phone_Spout extends BaseRichSpout {

	SpoutOutputCollector collector = null;
	String [] phones = {"Xiaomi","Huawei","Zhongxing","Meizu","Oppo","Vivo"};
	
	@Override
	public void nextTuple() {
		// TODO Auto-generated method stub
		Random random = new Random();
		String phone = phones[random.nextInt(phones.length)];
		collector.emit(new Values(phone));
		
	}

	@Override
	public void open(Map arg0, TopologyContext arg1, SpoutOutputCollector arg2) {
		// TODO Auto-generated method stub
		collector = arg2;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		// TODO Auto-generated method stub
		arg0.declare(new Fields("Phone"));
	}

}
