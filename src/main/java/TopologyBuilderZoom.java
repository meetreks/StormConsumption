import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;


public class TopologyBuilderZoom  {

    public static void main(String[] args) throws InterruptedException {
        TopologyBuilder tp = new TopologyBuilder();
        tp.setSpout("Yahoo-Finance-Spout", new SpoutZoom());
        tp.setBolt("YahooBolt", new BoltZoom()).shuffleGrouping("Yahoo-Finance-Spout");
        StormTopology st = tp.createTopology();


        Config cf = new Config();
        cf.setDebug(true);
        cf.put("fileToWrite", "/StormConsumption/Output.txt");
        //LocalCluster lc = new LocalCluster();
        try {
            StormSubmitter.submitTopology("Sai Topology", cf, st);

        } catch (Exception e) {
            System.out.println(e.toString());
        }
    }
}
