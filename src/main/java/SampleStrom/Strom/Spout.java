package SampleStrom.Strom;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import yahoofinance.YahooFinance;
import yahoofinance.quotes.stock.StockQuote;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Map;

public class Spout extends BaseRichSpout {
    private SpoutOutputCollector collector;
    private SimpleDateFormat sdf = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss");
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = collector;
    }

    public void nextTuple() {
        try{
            StockQuote quote = YahooFinance.get("MFST").getQuote();
            BigDecimal price = quote.getPrice();
            BigDecimal previousClose = quote.getPreviousClose();
            Timestamp timestamp = new Timestamp(System.currentTimeMillis());
            collector.emit(new Values("MFST", sdf.format(timestamp),price.doubleValue(),previousClose.doubleValue()));

        }catch (Exception e){

        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("company", "timestamp","price","prev-close"));

    }
}
