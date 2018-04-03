package com.codebind;

import java.util.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;

public class KafkaConsumerTwo {

   public static void main(String[] args) throws Exception{

      String topicName = "logReader";

      Properties props = new Properties();
      props.put("bootstrap.servers", "localhost:9092");
      props.put("application.id","logcounter");
      props.put("group.id","test");
//      props.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
//      props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
      props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
      props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
      props.put("cache.max.bytes.buffering","0");

      KStreamBuilder ks = new KStreamBuilder();
      KStream<String,String> logreader = ks.stream(topicName);
      
      KStream<String,String> [] arrayOfLogStreams = logreader.branch(
		  (key, value) -> value.contains("INFO"),
		  (key, value) -> value.contains("ERROR"),
		  (key, value) -> value.contains("DEBUG")
		  );

      
     logreader.map((Key,value) -> {
   	  if(value.contains("INFO")){
  		  return new KeyValue<>("INFO","1");
    	  }else if(value.contains("ERROR")){
    		  return new KeyValue<>("ERROR","1");
    	  }else
    		  return new KeyValue<>("OTHER","1");
      }
      );
      
      for (KStream specificLogStream : arrayOfLogStreams){
    	  System.out.print("Key " + " Value : " + specificLogStream.groupByKey().count());
    	  specificLogStream.groupByKey().count().print();
      }
      
      
      KafkaStreams streams = new KafkaStreams(ks,props);
      streams.start();
   }
   }

   
   
