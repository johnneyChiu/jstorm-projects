package com.sijian.topology;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Map;

public class FirstTestSpout implements IRichSpout {

    private static final long serialVersionUID = 4058847280819269954L;
    private static final Logger logger = LoggerFactory.getLogger(FirstTestSpout.class);



    private FileReader fileReader;
    private boolean complete=false;


    private SpoutOutputCollector outputCollector;

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        try {

            this.fileReader = new FileReader(map.get("infile").toString());
        } catch (FileNotFoundException e) {
            logger.info("this file " + map.get("infile").toString() + " is not found！");
        }
        this.outputCollector = spoutOutputCollector;
    }

    @Override
    public void close() {

    }

    @Override
    public void activate() {
        logger.debug("ok,program is activing");
    }

    @Override
    public void deactivate() {

    }
    //这是Spout最主要的方法，在这里我们读取文本文件，并把它的每一行发射出去（给bolt）
    @Override
    public void nextTuple() {
        if (complete){
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return;
        }
        String str;
        BufferedReader br = new BufferedReader(fileReader);
        try{
            while ((str=br.readLine())!=null){
                //发射每一行，Values是一个ArrayList的实现
                logger.info(str);
                this.outputCollector.emit(new Values(str), str);
            }
        }catch (Exception e){
            logger.info("Error reading tuple", e);
        }finally {
            complete = true;
        }
    }

    @Override
    public void ack(Object o) {
        logger.info("this is ack"+o.toString());
        System.out.println("ack is ok:"+o.toString());
    }

    @Override
    public void fail(Object o) {
        logger.info("Fail");
        System.out.println("fail:"+o.toString());
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("line"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
