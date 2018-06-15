package net.qyjohn.KinesisTutorials;

import java.io.*;
import java.util.*;
import java.nio.ByteBuffer;
import com.amazonaws.services.kinesis.producer.*;


public class KPLExample extends Thread
{
        public KinesisProducer client;
	public String streamName;

        public KPLExample()
        {
		KinesisProducerConfiguration config = new KinesisProducerConfiguration();
		config.setRegion("ap-southeast-2");
		config.setLogLevel("info");		
		client = new KinesisProducer(config);
		try
		{
			Properties prop = new Properties();
			InputStream input = new FileInputStream("kinesis.properties");
			prop.load(input);
			streamName = prop.getProperty("streamName");
		}catch (Exception e)
		{
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
        }

	public void run()
	{
		while (true)
		{
			try
			{
				String uuid = UUID.randomUUID().toString();
				ByteBuffer data = ByteBuffer.wrap(uuid.getBytes());
				// Use UUID for both the data and partition key
				client.addUserRecord(streamName, uuid, data);
			} catch (Exception e) 
			{
				System.out.println(e.getMessage());
				e.printStackTrace();
			}
		}
	}

        public static void main(String[] args)
        {
		try 
		{
			KPLExample demo = new KPLExample();
			demo.run();
		} catch (Exception e) 
		{
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
        }
}
