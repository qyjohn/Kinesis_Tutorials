package net.qyjohn.KinesisTutorials;

import java.util.*;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.*;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.*;
import com.amazonaws.services.kinesis.model.Record;


public class KCLRecordProcessor implements IRecordProcessor
{
	@Override
	public void initialize(String shardId) 
	{
	}

	@Override
	public void processRecords(List<Record> records, IRecordProcessorCheckpointer checkpointer) 
	{
		for (Record r : records) 
		{
			// Do your record processing here.
			System.out.println(r.getPartitionKey());
		}
		try 
		{
			checkpointer.checkpoint();
		} catch (Exception e) 
		{
			
		}
	}

	@Override
	public void shutdown(IRecordProcessorCheckpointer checkpointer, ShutdownReason reason) 
	{
		try 
		{
			checkpointer.checkpoint();
		} catch (Exception e) 
		{
		}
	}
}
