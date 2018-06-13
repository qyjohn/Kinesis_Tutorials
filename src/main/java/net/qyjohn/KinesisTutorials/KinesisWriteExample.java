package net.qyjohn.KinesisTutorials;

import java.io.*;
import java.util.*;
import java.nio.charset.Charset;
import java.nio.ByteBuffer;
import com.amazonaws.*;
import com.amazonaws.auth.*;
import com.amazonaws.auth.profile.*;
import com.amazonaws.regions.*;
import com.amazonaws.services.kinesis.*;
import com.amazonaws.services.kinesis.model.*;


public class KinesisWriteExample extends Thread
{
	AmazonKinesisClient client;
	String streamName;

	public KinesisWriteExample(String streamName)
	{
		client = new AmazonKinesisClient();
		client.configureRegion(Regions.AP_SOUTHEAST_2);
		this.streamName = streamName;
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
				client.putRecord(streamName, data, uuid);
			} catch (Exception e) 
			{
				System.out.println(e.getMessage());
				e.printStackTrace();
			}
		}
	}

	public static void main(String[] args)
	{
		String streamName = args[0];
		KinesisWriteExample example = new KinesisWriteExample(streamName);
		example.start();
	}
}
