package com.vmware.dim.impl;

import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.vmware.common.dto.ConfigurationDetailsDTO;
import com.vmware.dim.conn.MongoDBConnection;
import com.vmware.dim.output.OutputStreamWriter;

/**
 * MongoDB output target class implementing {@link OutputStreamWriter}<br>
 * Reads the data from Redis and pushes to MongoDB
 * 
 * @author ghimanshu
 *
 */
public class MongoDBOutputStream extends OutputStreamWriter {

	private static final Logger logger = LogManager.getLogger(MongoDBOutputStream.class);

	private MongoDBConnection mongoDBConnection;
	private MongoCollection<Document> mongoCollection;

	public MongoDBOutputStream(ConfigurationDetailsDTO confDetailsDTO) throws Exception {
		super(confDetailsDTO);
		logger.traceEntry();

		mongoDBConnection = new MongoDBConnection(configuration.get("clientURI"));
		MongoClient mongoClient = mongoDBConnection.getConnection();
		MongoDatabase mongoDatabase = mongoClient.getDatabase(configuration.get("db"));
		mongoCollection = mongoDatabase.getCollection(configuration.get("collection"));

		logger.traceExit();
	}

	/**
	 * Method to write data to MongoDB for each Redis key and value
	 */
	@Override
	public void write(Map<String, String> redisData) {

		logger.traceEntry();

		for (String name : redisData.keySet()) {
			String value = redisData.get(name);
			try {
				upsert(mongoCollection, name, value);
			} catch (Exception e) {
				logger.error("Error Upserting data in MongoDB", e);
			}
		}
		logger.traceExit();
	}

	private void upsert(MongoCollection<Document> collection, String key, String json) {

		logger.traceEntry(String.valueOf(collection.count()), key, json);
		Document document = collection.find(new Document("_id", key)).first();

		if (document == null) {
			Document document2 = Document.parse(json);
			document2.append("_id", key);
			collection.insertOne(document2);
		} else {
			collection.updateOne(new Document("_id", key), new Document("$set", new Document(Document.parse(json))));
		}
		logger.traceExit();
	}

}