package com.sijian.topology;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import com.alibaba.jstorm.callback.AsyncLoopThread;
import com.alibaba.jstorm.callback.RunnableCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

public class FirstTestBoltWordCount implements IRichBolt {

    private Logger logger = LoggerFactory.getLogger(FirstTestBoltWordCount.class);


    private Map<String, Integer> countMap;
    private OutputCollector outputCollector;
    String name;
    Integer id;
    private AsyncLoopThread statThread;


    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
        this.id = topologyContext.getThisTaskId();
        this.name = topologyContext.getThisComponentId();
        this.countMap = new HashMap<>();

        logger.info(map.get("starting")+"!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
        this.statThread = new AsyncLoopThread(new TestOutCallBack("callback", map.get("outfile").toString()));

    }

    @Override
    public void execute(Tuple tuple) {
        String str = tuple.getString(0);
        logger.info("data is :"+str);
        if (!countMap.containsKey(str)) {
            countMap.put(str, 1);
        }
        else
            countMap.put(str, countMap.get(str) + 1);
        this.outputCollector.ack(tuple);
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    class TestOutCallBack extends RunnableCallback{
        private String name;
        private String callbackPath;
        private boolean flag=true;

        public TestOutCallBack(String name, String callbackPath) {
            this.name = name;
            this.callbackPath = callbackPath;
        }
        public void run(){
            while (flag) {
                System.out.println("callbacking,try to waiting 20 sec");
                logger.info("haha,starting");

                try{
                    Thread.sleep(20000);

                }catch (InterruptedException ex){
                    throw new RuntimeException(ex);
                }
                FileWriter fileWriter = null;
                try {
                    //BufferedOutputStream bo = new BufferedOutputStream(new FileOutputStream(new File(this.callbackPath)));
                    fileWriter = new FileWriter(this.callbackPath);
                    logger.info("test");
                    for (Map.Entry<String, Integer> entry : countMap.entrySet()) {
                        fileWriter.write(entry.getKey()+":"+entry.getValue()+"\r\n");
                        logger.info(entry.getKey() + ":" + entry.getValue() + "\r\n");
                    }
                    flag = false;
                } catch (IOException e) {
                    e.printStackTrace();
                }finally {
                    if (fileWriter != null) {
                        try {
                            fileWriter.close();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                }

            }
        }
    }
}
