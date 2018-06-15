package net.qyjohn.KinesisTutorials;

import java.io.*;
import java.util.*;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.*;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.*;
import com.amazonaws.services.kinesis.model.Record;

public class KCLExample implements IRecordProcessorFactory
{
	@Override
	public IRecordProcessor createProcessor()
	{
		return new KCLRecordProcessor();
	}
    
	public static void main(String[] args) 
	{

		try
		{
			Properties prop = new Properties();
			InputStream input = new FileInputStream("kcl.properties");
			prop.load(input);
			String applicationName = prop.getProperty("applicationName");
			String streamName = prop.getProperty("streamName");
			String workerName = InetAddress.getLocalHost().getCanonicalHostName() + ":" + UUID.randomUUID();

			KinesisClientLibConfiguration config = new KinesisClientLibConfiguration(
				applicationName,
				streamName,
				new DefaultAWSCredentialsProviderChain(),
				workerName)
				.withRegionName("ap-southeast-2")
				.withInitialPositionInStream(InitialPositionInStream.TRIM_HORIZON);
        
			KCLExample consumer = new KCLExample();
			new Worker.Builder()
			    .recordProcessorFactory(consumer)
			    .config(config)
			    .build()
			    .run();

		}catch (Exception e)
		{
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
	}
}
