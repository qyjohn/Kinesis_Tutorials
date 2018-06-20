package net.qyjohn.KinesisTutorials;



import java.io.*;
import java.util.*;
import java.nio.charset.Charset;
import java.sql.*;
import com.amazonaws.*;
import com.amazonaws.auth.*;
import com.amazonaws.auth.profile.*;
import com.amazonaws.regions.*;
import com.amazonaws.services.kinesis.*;
import com.amazonaws.services.kinesis.model.*;


public class WorkerDemo
{
	public String workerId, myShardId, seqNumber;
	public String streamName;
	public AmazonKinesisClient client;
	public String dbHostname, dbDatabase, dbUsername, dbPassword;
	public Connection conn;
	public String sql;

	public WorkerDemo()
	{
		client = new AmazonKinesisClient();            
		client.configureRegion(Regions.AP_SOUTHEAST_2);
 		loadProperties();
		System.out.println(streamName);
		
		workerId = UUID.randomUUID().toString();  
		System.out.println(workerId);
		
		initLeaseTable();
		getLease();
		
	}	
	
	public void loadProperties()
	{
		try
		{
			Properties prop = new Properties();
			InputStream input = new FileInputStream("kinesis.properties");
			prop.load(input);
			streamName = prop.getProperty("streamName");
			dbHostname = prop.getProperty("dbHostname");
			dbDatabase = prop.getProperty("dbDatabase");
			dbUsername = prop.getProperty("dbUsername");
			dbPassword = prop.getProperty("dbPassword");
			
			Class.forName("com.mysql.jdbc.Driver");
			String jdbcUrl = "jdbc:mysql://" + dbHostname + "/" + dbDatabase + "?user=" + dbUsername + "&password=" + dbPassword;
			conn = DriverManager.getConnection(jdbcUrl);						
		}catch (Exception e)
		{
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
	}
	
	public void initLeaseTable()
	{
		try
		{
			DescribeStreamResult result = client.describeStream(streamName);
			StreamDescription description = result.getStreamDescription();
			List<Shard> shards = description.getShards();
			for (Shard shard : shards)
			{
				String shardId = shard.getShardId();
				sql = "INSERT IGNORE INTO leases (shard_id, ts, lease_counter) VALUES (?, now(), 0)";
				PreparedStatement preparedStatement = conn.prepareStatement(sql);
				preparedStatement.setString(1, shardId);
				preparedStatement.executeUpdate();				
			}
		}catch (Exception e)
		{
			System.out.println(e.getMessage());
			e.printStackTrace();
		}		
	}
	
	public void getLease()
	{
		try
		{
			// Get lease table states
			// SELECT * FROM leases;
			String sql = "SELECT * FROM leases";
			Statement statement = conn.createStatement();
			ResultSet resultSet = statement.executeQuery(sql);
			while (resultSet.next())
			{
				String shardId = resultSet.getString("shard_id");
				String currentWorkerId = resultSet.getString("worker_id");
				Timestamp ts = resultSet.getTimestamp("ts");
				int counter = resultSet.getInt("lease_counter");
				seqNumber = resultSet.getString("checkpoint");
				int closed = resultSet.getInt("closed");
				System.out.println(shardId + "\t" + currentWorkerId + "\t" + ts);
				
				Timestamp now = new Timestamp(System.currentTimeMillis());
				long elapse = (now.getTime() - ts.getTime()) / 1000;
				System.out.println(elapse);
				
				if (((currentWorkerId==null) || (elapse > 120L)) && (closed == 0))
				{
					System.out.println("Attempting to lock shard " + shardId);
					sql = "UPDATE leases SET worker_id=?, ts=now(), lease_counter=lease_counter+1 WHERE shard_id=? AND lease_counter=?";
					PreparedStatement preparedStatement = conn.prepareStatement(sql);
					System.out.println(workerId);
					preparedStatement.setString(1, workerId);
					preparedStatement.setString(2, shardId);
					preparedStatement.setInt(3, counter);
					int result = preparedStatement.executeUpdate();	
					if (result == 1)
					{
						System.out.println("Locked " + shardId);
						myShardId = shardId;
						processRecord();
					}
					break;
				}
			}
					
		}catch (Exception e)
		{
			System.out.println(e.getMessage());
			e.printStackTrace();
		}	
	}
	
	public void processRecord()
	{
		try
		{
			String shardIterator = "null";
			if (seqNumber == null)
			{
				GetShardIteratorResult result1 = client.getShardIterator(streamName, myShardId, "TRIM_HORIZON");
				shardIterator = result1.getShardIterator();
				
			}
			else
			{
				GetShardIteratorResult result1 = client.getShardIterator(streamName, myShardId, "AFTER_SEQUENCE_NUMBER", seqNumber);
				shardIterator = result1.getShardIterator();
			}

			
			boolean hasData = true;
			while (hasData)
			{
				System.out.println(shardIterator);
				GetRecordsResult result2 = client.getRecords(new GetRecordsRequest().withShardIterator(shardIterator));	
				shardIterator = result2.getNextShardIterator();
				if (shardIterator == null)
				{
					hasData = false; // Shard closed.
				}

				List<Record> records = result2.getRecords();
				System.out.println("Found " + records.size() + " records.");
				if (!records.isEmpty())
				{
					String seq = "";
					for (Record record : records)
					{
						seq = record.getSequenceNumber();
						String data = Charset.forName("UTF-8").decode(record.getData()).toString();
						System.out.println(myShardId + "\t" + data);
					}
					
					// Check point after processing records
					sql = "UPDATE leases SET checkpoint=?, ts=now() WHERE shard_id=?";
					PreparedStatement preparedStatement = conn.prepareStatement(sql);
					preparedStatement.setString(1, seq);
					preparedStatement.setString(2, myShardId);
					int result = preparedStatement.executeUpdate();						
				}
				else
				{
					Thread.sleep(20);
				}
			}
			
			// Shard closed
			sql = "UPDATE leases SET closed=1, ts=now() WHERE shard_id=?";
			PreparedStatement preparedStatement = conn.prepareStatement(sql);
			preparedStatement.setString(1, myShardId);
			preparedStatement.executeUpdate();						
			
		}catch (Exception e)
		{
			System.out.println(e.getMessage());
			e.printStackTrace();
		}		
	}
	
	
	public static void main(String[] args)
	{
		WorkerDemo demo = new WorkerDemo();
		
	}
}

