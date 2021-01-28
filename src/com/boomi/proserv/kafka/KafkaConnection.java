package com.boomi.proserv.kafka;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.time.Duration;
import java.util.*;
import java.util.logging.Logger;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.boomi.document.scripting.DataContextImpl;
import com.boomi.execution.ExecutionUtil;
import com.boomi.proserv.kafka.pooling.ConsumerFactory;
import com.boomi.proserv.kafka.pooling.ConsumerPool;
import com.boomi.proserv.kafka.pooling.ProducerFactory;
import com.boomi.proserv.kafka.pooling.ProducerPool;
import org.apache.kafka.common.TopicPartition;

/**
 * KafkaConnection.getConnection("<Server>").sendDocuments("<topicName>", dataContext);
 * 
 * KafkaConnection.getConnection("<Server>").getDocuments("<topicName>", dataContext, <pollingTime in milliseconds>);
 * 
 * or with Connection Polling
 * 
 * KafkaConnection.getConnection("<Server>", true, <maxIdle>, <maxConnection>, <groupId>).sendDocuments("<topicName>", dataContext);
 * 
 * KafkaConnection.getConnection("<Server>", true, <maxIdle>, <maxConnection>, <groupId>).getDocuments("<topicName>", dataContext, <pollingTime in milliseconds>, <assign Boolean>);
 * 
 * @author anthony.rabiaza
 *
 */
public class KafkaConnection {

	private String server;
	private Properties properties;
	private boolean pooling;
	private int maxIdle;
	private int maxConnection;
	private static boolean localExecution = false;

	static ProducerFactory s_producerFactory;
	static ProducerPool s_producerPool;

	static ConsumerFactory s_consumerFactory;
	static ConsumerPool s_consumerPool;

	public KafkaConnection(Properties properties, boolean pooling, int maxIdle, int maxConnection) {
		this.properties = properties;
		this.server = properties.getProperty("bootstrap.servers");
		this.pooling = pooling;
		this.maxIdle = maxIdle;
		this.maxConnection = maxConnection;
	}

	public KafkaConnection(String server, boolean pooling, int maxIdle, int maxConnection, String groupId) {
		properties = new Properties();
		properties.put("bootstrap.servers", server);
		properties.put("acks", "all");
		if(groupId!=null) {
			properties.put("group.id", groupId);
		}
		properties.put("retries", 0);
		properties.put("batch.size", 16384);
		properties.put("linger.ms", 1);
		properties.put("buffer.memory", 33554432);
		properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		this.server = server;
		this.pooling = pooling;
		this.maxIdle = maxIdle;
		this.maxConnection = maxConnection;
	}

	public void addProperty(String key, String value) {
		properties.put(key, value);
	}

	public String getServer() {
		return server;
	}

	public Properties getProperties() {
		return properties;
	}

	public boolean isPooling() {
		return pooling;
	}

	public int getMaxIdle() {
		return maxIdle;
	}

	public int getMaxConnection() {
		return maxConnection;
	}

	public static KafkaConnection getConnection(String server) {
		return new KafkaConnection(server, false, 0, 0, null);
	}

	public static KafkaConnection getConnection(Properties properties) {
		return new KafkaConnection(properties, false, 0, 0);
	}

	public static KafkaConnection getConnection(String server, boolean pooling, int maxIdle, int maxConnection) {
		return new KafkaConnection(server, pooling, maxIdle, maxConnection , null);
	}

	public static KafkaConnection getConnection(String server, boolean pooling, int maxIdle, int maxConnection, String groupId) {
		return new KafkaConnection(server, pooling, maxIdle, maxConnection, groupId);
	}

	public static KafkaConnection getConnection(Properties properties, boolean pooling, int maxIdle, int maxConnection) {
		return new KafkaConnection(properties, pooling, maxIdle, maxConnection);
	}

	public void sendDocuments(String topicName, DataContextImpl dataContext) throws Exception {
		Producer<String, String> producer = getProducer();
		try {
			for (int i = 0; i < dataContext.getDataCount(); i++) {
				InputStream is = dataContext.getStream(i);

				ByteArrayOutputStream baos = new ByteArrayOutputStream();
				byte[] buffer = new byte[1024];
				int length;
				
				while ((length = is.read(buffer)) != -1) {
					baos.write(buffer, 0, length);
				}
				
				String docString = baos.toString("UTF-8");
				sendDocuments(topicName, docString);
			}
		} finally {
			if(pooling) {
				getLogger().info("Returning Connection to Producer Pool");
				s_producerPool.returnObject(producer);
			} else {
				getLogger().info("Closing Kafka Connection");
				producer.close();
			}
		}
	}

	public void sendDocuments(String topicName, String docString) throws Exception {
		Producer<String, String> producer = getProducer();
		try {
			producer.send(new ProducerRecord<String, String>(topicName, docString));
		} finally {
			if(pooling) {
				getLogger().info("Returning Connection to Producer Pool");
				s_producerPool.returnObject(producer);
			} else {
				getLogger().info("Closing Kafka Connection");
				producer.close();
			}
		}
	}

	public void getDocuments(String topicName, DataContextImpl dataContext, int pollingTime, boolean assign) throws Exception {
		List<String> documents = getDocuments(topicName, pollingTime, assign);
		for (int i = 0; i < documents.size(); i++) {
			dataContext.storeStream(new ByteArrayInputStream(documents.get(i).getBytes()), new Properties());
		}
	}

	public List<String> getDocuments(String topicName, int pollingTime, boolean assign) throws Exception {
		Consumer<String, String> consumer = getConsumer();
		List<String> documents = new ArrayList<String>();
		try {
			// Get records from topic
			getLogger().info("Subscribing to topic "+topicName+"...");
			if(assign) {
				TopicPartition topicPartition = new TopicPartition(topicName, 0);
				List<TopicPartition> topicPartitions = Arrays.asList(topicPartition);
				consumer.assign(topicPartitions);
			} else {
				consumer.subscribe(Arrays.asList(topicName));
			}
			getLogger().info("Polling for "+pollingTime+" seconds...");
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(pollingTime));
			getLogger().info("Record received");
			// Get each record value
			String msg = "";
			for (ConsumerRecord<String, String> record : records) {
				msg = record.value();
				documents.add(msg);
			}
			consumer.commitAsync();
			return documents;
		} finally {
			if(pooling) {
				getLogger().info("Returning Connection to Consumer Pool");
				s_consumerPool.returnObject(consumer);
			} else {
				getLogger().info("Closing Kafka Connection");
				consumer.close();
			}
		}
	}

	protected Producer<String, String> getProducer() throws Exception {
		Producer<String, String> producer;
		if(server==null || "".equals(server)) {
			getLogger().severe("Kafka Server name is " + server);
		}

		if(pooling) {
			if(s_producerFactory==null || s_producerPool==null) {
				getLogger().info("Initialization of Kafka ProducerPool ...");
				GenericObjectPoolConfig config = new GenericObjectPoolConfig();
				config.setMaxIdle(maxIdle);
				config.setMaxTotal(maxConnection);
				config.setTestOnBorrow(true);
				config.setTestOnReturn(true);
				s_producerFactory = new ProducerFactory();
				s_producerFactory.setProperties(properties);
				s_producerPool = new ProducerPool(s_producerFactory, config);
				getLogger().info("Initialization of Kafka ProducerPool done");
			}
			getLogger().info("Borrowing Connection from Kafka ConsumerPool ...");
			producer = s_producerPool.borrowObject();
		} else {
			getLogger().info("Creating New Kafka Connection ...");
			producer = new KafkaProducer<String, String>(properties);
		}
		getLogger().info("Kafka Connection producer created");
		return producer;
	}

	protected Consumer<String, String> getConsumer() throws Exception {
		Consumer<String, String> consumer;
		if(server==null || "".equals(server)) {
			getLogger().severe("Kafka Server name is " + server);
		}

		if(pooling) {
			if(s_consumerFactory==null || s_consumerPool==null) {
				getLogger().info("Initialization of Kafka ConsumerPool ...");
				GenericObjectPoolConfig config = new GenericObjectPoolConfig();
				config.setMaxIdle(maxIdle);
				config.setMaxTotal(maxConnection);
				config.setTestOnBorrow(true);
				config.setTestOnReturn(true);
				s_consumerFactory = new ConsumerFactory();
				s_consumerFactory.setProperties(properties);
				s_consumerPool = new ConsumerPool(s_consumerFactory, config);
				getLogger().info("Initialization of Kafka ConsumerPool done");
			}
			getLogger().info("Borrowing Connection from Kafka ConsumerPool ...");
			consumer = s_consumerPool.borrowObject();
		} else {
			getLogger().info("Creating New Kafka Connection ...");
			consumer = new KafkaConsumer<String, String>(properties);
		}
		getLogger().info("Kafka Connection consumer created");
		return consumer;
	}

	protected void reset() {
	}

	private static Logger getLogger() {
		try {
			if(isLocalExecution()) {
				return Logger.getLogger(KafkaConnection.class.getName());
			} else {
				return ExecutionUtil.getBaseLogger();
			}
		} catch (Exception e) {
			return Logger.getLogger(KafkaConnection.class.getName());
		}
	}

	public static boolean isLocalExecution() {
		return localExecution;
	}

	public static void setLocalExecution(boolean localExecution) {
		KafkaConnection.localExecution = localExecution;
	}


}
