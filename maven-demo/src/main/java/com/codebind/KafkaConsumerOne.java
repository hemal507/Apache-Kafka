package com.codebind;

import java.util.*;
import org.apache.kafka.clients.consumer.*;

public class KafkaConsumerOne {

   public static void main(String[] args) throws Exception{

      String topicName = "Contact";

      Properties props = new Properties();
      props.put("bootstrap.servers", "localhost:9092");
      props.put("group.id","test");
      props.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
      props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

      KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
      try{
      	  consumer.subscribe(Arrays.asList(topicName));

	  while(true) {
	  ConsumerRecords<String, String> records = consumer.poll(10);
	  for(ConsumerRecord<String, String> record : records){
		  System.out.println("Key: " + record.key().toString() + "\t Value : " + record.value().toString());
	  }
	  consumer.commitAsync();
	}
	
     }catch (Exception e){
	      e.printStackTrace();
	} finally {      
	  consumer.close();
	  System.out.println("Consumer process Completed.");
   }
   }
   }
   
   
