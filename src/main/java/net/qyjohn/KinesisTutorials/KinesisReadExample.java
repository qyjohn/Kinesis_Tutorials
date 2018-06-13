package net.qyjohn.KinesisTutorials;

import java.io.*;
import java.util.*;
import java.nio.charset.Charset;
import com.amazonaws.*;
import com.amazonaws.auth.*;
import com.amazonaws.auth.profile.*;
import com.amazonaws.regions.*;
import com.amazonaws.services.kinesis.*;
import com.amazonaws.services.kinesis.model.*;

class ShardReader extends Thread
{
        AmazonKinesisClient client;
	String streamName, shardId;

	public ShardReader(String streamName, String shardId)
	{
                client = new AmazonKinesisClient();
                client.configureRegion(Regions.AP_SOUTHEAST_2);
		this.streamName = streamName;
		this.shardId = shardId;
	}

	public void run()
	{
		try
		{
			GetShardIteratorResult result1 = client.getShardIterator(streamName, shardId, "TRIM_HORIZON");
			String shardIterator = result1.getShardIterator();

			
			boolean hasData = true;
			while (hasData)
			{
				GetRecordsResult result2 = client.getRecords(new GetRecordsRequest().withShardIterator(shardIterator));	
				shardIterator = result2.getNextShardIterator();
				if (shardIterator == null)
				{
					hasData = false; // Shard closed.
				}

				List<Record> records = result2.getRecords();
				if (records.isEmpty())
				{
					sleep(2000);	// No records
				}
				else
				{
					for (Record record : records)
					{
						String data = Charset.forName("UTF-8").decode(record.getData()).toString();
						System.out.println(shardId + "\t" + data);
					}
				}
			}
		}catch (Exception e)
		{
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
	}
}

public class KinesisReadExample
{
        public AmazonKinesisClient client;
	public String streamName;

        public KinesisReadExample()
        {
                client = new AmazonKinesisClient();
                client.configureRegion(Regions.AP_SOUTHEAST_2);
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
		try
		{
			DescribeStreamResult result = client.describeStream(streamName);
			StreamDescription description = result.getStreamDescription();
			List<Shard> shards = description.getShards();
			for (Shard shard : shards)
			{
				String shardId = shard.getShardId();
				new ShardReader(streamName, shardId).start();
			}
		}catch (Exception e)
		{
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
	}

        public static void main(String[] args)
        {
		try 
		{
			KinesisReadExample demo = new KinesisReadExample();
			demo.run();
		} catch (Exception e) 
		{
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
        }
}
