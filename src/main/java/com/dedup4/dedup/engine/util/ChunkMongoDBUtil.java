package com.dedup4.dedup.engine.util;

import static com.mongodb.client.model.Filters.eq;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dedup4.dedup.engine.exception.DedupEngineException;
import com.dedup4.dedup.engine.exception.ExceptionMsg;
import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.gridfs.GridFSBucket;
import com.mongodb.client.gridfs.GridFSBuckets;
import com.mongodb.client.gridfs.model.GridFSFile;
import com.mongodb.client.gridfs.model.GridFSUploadOptions;

public class ChunkMongoDBUtil {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(ChunkMongoDBUtil.class);
	
	private static final String DEFAULT_CHUNK_FILENAME_PREFIX = "chunk_";
	private static final int 	DEFAULT_CHUNK_SIZE_BYTES = 1024;
	private static final int 	DEFAULT_MONGODB_PORT 	 = 27017;
	private static final String DEFAULT_MONGODB_HOST 	 = "127.0.0.1";
	private static final String DEFAULT_CHUNK_DB_NAME 	 = "ChunkDb";

	private MongoClient connection;
	private MongoDatabase chunkDb;
	private GridFSBucket chunkGridFSBucket;
	private String mongoDbHost;
	private int mongoDbPort;
	private String chunkDbName;
	private int chunkSizeBytes;
	private String chunkFilenamePrefix;
	
	private static ChunkMongoDBUtil INSTANCE;
	
	public static ChunkMongoDBUtil getInstance() {
		if(INSTANCE == null) {
			synchronized (ChunkMongoDBUtil.class) {
				if(INSTANCE == null) {
					LOGGER.info("Creating ChunkMongoDBUtil Instance.");
					
					INSTANCE = new ChunkMongoDBUtil();
					INSTANCE.loadProperties();
					INSTANCE.initGridFSBucket();
				}
			}
		}
		LOGGER.info("Finished creating ChunkMongoDBUtil Instance.");
		return INSTANCE;
	}
	
	private void loadProperties() {
		Properties mongoProperties = new Properties();
		InputStream in = getClass().getResourceAsStream("/mongo.properties");
		try {
			mongoProperties.load(in);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		String host = (String)mongoProperties.get("mongoDbHost");
		mongoDbHost = (host == null ? DEFAULT_MONGODB_HOST : host);
		
		String portStr = (String)mongoProperties.get("mongoDbPort");
		mongoDbPort = (portStr == null ? DEFAULT_MONGODB_PORT : Integer.valueOf(portStr));

		String dbName = (String)mongoProperties.get("chunkDbName");
		chunkDbName = (dbName == null ? DEFAULT_CHUNK_DB_NAME : dbName);
		
		String chunkSizeBytesStr = (String)mongoProperties.get("chunkSizeBytes");
		chunkSizeBytes = (chunkSizeBytesStr == null ? DEFAULT_CHUNK_SIZE_BYTES : Integer.valueOf(chunkSizeBytesStr));

		String prefix = (String)mongoProperties.get("chunkFilenamePrefix");
		chunkFilenamePrefix = (prefix == null ? DEFAULT_CHUNK_FILENAME_PREFIX : prefix);
		
		LOGGER.info("ChunkMongoDBUtil Properties[ host : {}, port : {}, database : {}, chunkSizeBytes : {}, prefix : {} ]", 
				mongoDbHost, mongoDbPort, chunkDbName, chunkSizeBytes, chunkFilenamePrefix);
	}
	
	private void initGridFSBucket() {
		connection = new MongoClient(mongoDbHost, mongoDbPort);
		chunkDb = connection.getDatabase(chunkDbName);
		chunkGridFSBucket = GridFSBuckets.create(chunkDb);
	}
	
	/**
	 * Upload chunk to MongoDB.
	 * @param chunkId
	 * @param chunkContent
	 * @throws IOException 
	 */
	public void uploadChunk(long chunkId, byte[] chunkContent) throws IOException {
		LOGGER.info("Uploading chunk (ID = {0}) to MongoDB.", chunkId);
		
		InputStream streamToUploadFrom = new ByteArrayInputStream(chunkContent);
		GridFSUploadOptions options = new GridFSUploadOptions().chunkSizeBytes(chunkSizeBytes);
		chunkGridFSBucket.uploadFromStream(chunkFilenamePrefix + chunkId, streamToUploadFrom, options);
		streamToUploadFrom.close();
	}
	
	/**
	 * Download chunk from MongoDB.
	 * @param chunkId
	 * @return
	 * @throws IOException
	 */
	public byte[] downloadChunk(long chunkId) throws IOException {
		LOGGER.info("Downloading chunk (ID = {0}) from MongoDB.", chunkId);
		
		ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        chunkGridFSBucket.downloadToStreamByName(chunkFilenamePrefix + chunkId, outputStream);
        byte[] chunkContent = outputStream.toByteArray();
        outputStream.close();
        return chunkContent;
	}

	/**
	 * Delete chunk in MongoDB.
	 * @param chunkId
	 */
	public void deleteChunk(long chunkId) {
		LOGGER.info("Deleting chunk (ID = {0}) in MongoDB.", chunkId);
		chunkGridFSBucket.delete(getObjectIdByChunkId(chunkId));
	}
	
	private ObjectId getObjectIdByChunkId(long chunkId) {
		final List<ObjectId> objectIds = new ArrayList<>();
		chunkGridFSBucket.find(eq("filename", chunkFilenamePrefix + chunkId)).forEach(
				new Block<GridFSFile>() {
					@Override
					public void apply(final GridFSFile gridFSFile) {
						objectIds.add(gridFSFile.getObjectId());
					}
				});
		
		if(objectIds.size() == 1) {
			return objectIds.get(0);
		}
		else if(objectIds.size() == 0)  {
			return null;
		} else {
			LOGGER.error(ExceptionMsg.EXCEPTION001.getExceptionMsg());
			throw new DedupEngineException(ExceptionMsg.EXCEPTION001);
		}
	}
}
