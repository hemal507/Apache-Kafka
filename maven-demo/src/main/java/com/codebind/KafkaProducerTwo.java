package com.codebind;

import java.io.*;
import java.util.*;
import org.apache.kafka.clients.producer.*;

public class KafkaProducerTwo {

	   public static void main(String[] args) throws Exception{

		      String topicName = "logReader";

		      Properties props = new Properties();
		      props.put("bootstrap.servers", "localhost:9092");
		      props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
		      props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		      String key = "key";
		      Producer<String, String> producer = new KafkaProducer(props);
			  
		      try{
		    	  BufferedReader br = new BufferedReader(new FileReader(new File("c:\\kafka_2.11-1.0.1\\logfile.txt")));
		    	  String line = null;
		    	  
		    	  while((line=br.readLine())!= null){
		    		  try{
		    			  producer.send(new ProducerRecord<String,String>(topicName,key,line));
		    			  System.out.println("Sending");
		    		  }catch (Exception e) {
		    			  System.out.println("Error in sending " + topicName + " Key " + key + " Value : " + line);
		    		  }
		    	  }
		      }catch (Exception e){
		    	  e.printStackTrace();
		      }
		      
		      producer.close();

			  System.out.println("SimpleProducer Completed.");
		   }
}
