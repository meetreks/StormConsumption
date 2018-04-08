package SampleStrom.Strom;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.PrintWriter;
import java.util.Map;

public class Bolt extends BaseBasicBolt {
    private PrintWriter writer;
    public void execute(Tuple input, BasicOutputCollector basicOutputCollector) {
        String symbol = input.getValue(0).toString();
        String timestamp = input.getString(1);

        Double price = (Double) input.getValueByField("price");
        Double prevclose = input.getDoubleByField("prev-close");

        Boolean gain = true;

        if(price <= prevclose)
        {
            gain = false;
        }
        basicOutputCollector.emit(new Values(symbol,timestamp,price,gain));
        writer.println(symbol+","+timestamp+""+price+""+gain);
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("company", "timestamp","price","gain"));

    }

    public void prepare(Map stromConf, TopologyContext context)
    {
        String filename = stromConf.get("fileToWrite").toString();
        try{
            this.writer = new PrintWriter(filename, "UTF-8");
        }catch (Exception e){

        }
    }

    public void cleanup()
    {
        writer.close();
    }
}

