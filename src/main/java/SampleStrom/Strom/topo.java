package SampleStrom.Strom;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;


public class topo  {

    public static void main(String[] args) throws InterruptedException {
        TopologyBuilder tp = new TopologyBuilder();
        tp.setSpout("Yahoo-Finance-Spout", new Spout());
        tp.setBolt("YahooBolt", new Bolt()).shuffleGrouping("Yahoo-Finance-Spout");
        StormTopology st = tp.createTopology();


        Config cf = new Config();
        cf.setDebug(true);
        cf.put("fileToWrite", "/StormConsumption/Output.txt");
        LocalCluster lc = new LocalCluster();
        try {
            lc.submitTopology("Stocks", cf, st);
            Thread.sleep(10000);
        } catch (Exception e) {
            System.out.println(e.toString());
        } finally {
            lc.shutdown();
        }
    }
}
