package com.codebind;

import java.util.*;
import org.apache.kafka.clients.producer.*;

public class KafkaProducerOne {

	   public static void main(String[] args) throws Exception{

		      String topicName = "Contact";

		      Properties props = new Properties();
		      props.put("bootstrap.servers", "localhost:9092");
		      props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
		      props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		      Producer<String, String> producer = new KafkaProducer(props);
			  for(int i = 0;i<10;i++) {
			  	producer.send(new ProducerRecord<String, String>(topicName,Integer.toString(i),Integer.toString(i)));
		      }
		      producer.close();

			  System.out.println("SimpleProducer Completed.");
		   }
}
