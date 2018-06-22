package net.qyjohn.KinesisTutorials;

import java.util.List;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.KinesisEvent;
import com.amazonaws.services.lambda.runtime.events.KinesisEvent.KinesisEventRecord;

public class LambdaExample
{
	public int counter;
	
	public LambdaExample()
	{
		this.counter = 0;	
	}
	
	public void demoHandler(KinesisEvent event)
	{
		List<KinesisEvent.KinesisEventRecord> records = event.getRecords();
		for(KinesisEvent.KinesisEventRecord rec : records)
		{	
			counter = counter + 1;
			String data = new String(rec.getKinesis().getData().array());
			System.out.println(counter + "\t" + data);
		}
	}
}