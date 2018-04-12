package message.database;

import message.server.config.NodeConf;
import message.server.raft.RaftNode;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Logger;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.Binary;

import raft.proto.Internal.LogEntry;
import raft.proto.Internal.MessagePayLoad;
import raft.proto.Internal.MessageReadPayload;
import raft.proto.Internal.UserPayload;

import com.google.protobuf.InvalidProtocolBufferException;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOptions;

public class NodeStateMongoDB {
	final static String DB_NAME = "275db";
	final static String NODE_STATE_COLLECTION_NAME = "node_state";
	final static String LOG_COLLECTION_NAME = "logs";
	
	final static String CURRENT_TERM = "currentTerm";
	final static String LAST_VOTED_TERM = "lastVotedTerm";
	final static String LAST_APPLIED = "lastApplied";
	
	final static String LOG_TERM = "term";
	final static String LOG_INDEX = "index";
	final static String LOG_PAYLOAD = "payload";
	final static String LOG_PAYLOAD_TYPE = "payload_type";
	final static int PAYLOAD_TYPE_USER = 0;
	final static int PAYLOAD_TYPE_MESSAGE = 1;
	final static int PAYLOAD_TYPE_MESSAGES_READ = 2;
	
	private static final int ID_IDX = 1;
	
	private static NodeStateMongoDB instance = null;
	
	MongoClient mongoClient = null;
	MongoDatabase database = null;
	MongoCollection<Document> logCollection = null;
	MongoCollection<Document> nodeStateCollection = null;
	
	Document nodeStateDocument = null;
	
	private NodeStateMongoDB() {
		Logger.getGlobal().info("connecting to mongodb for node states");
		NodeConf conf = RaftNode.getInstance().getState().getNodeConf();
		mongoClient = new MongoClient("127.0.0.1", conf.getMongoPort());
		database = mongoClient.getDatabase(DB_NAME);
		logCollection = database.getCollection(LOG_COLLECTION_NAME);
		nodeStateCollection = database.getCollection(NODE_STATE_COLLECTION_NAME);
	}
	
	public static NodeStateMongoDB getInstance() {
		if (instance == null) {
			instance = new NodeStateMongoDB();
		}
		return instance;
	}
	
	/********************************************************************************/
	/* Persists node state in mongodb */
	/********************************************************************************/
	public synchronized boolean saveNodeState(RaftNode.State state) {
		if(nodeStateDocument == null) {
			nodeStateDocument = new Document();
			nodeStateDocument.put("_id", ID_IDX);
		}
		
		nodeStateDocument.put(CURRENT_TERM, state.getCurrentTerm());
		nodeStateDocument.put(LAST_VOTED_TERM, state.getLastVotedTerm());
		nodeStateDocument.put(LAST_APPLIED, state.getLastApplied());
		
		try {
			Bson filter = Filters.eq("_id", ID_IDX);
			UpdateOptions options = new UpdateOptions().upsert(true);
			nodeStateCollection.replaceOne(filter, nodeStateDocument, options);
			Logger.getGlobal().info("node state saved");
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
		
		return true;
	}
	
	public synchronized boolean restoreNodeState(RaftNode.State state) {
		try {
			nodeStateDocument = nodeStateCollection.find().first();
		} catch(Exception e) {
			e.printStackTrace();
		}
		
		if (nodeStateDocument == null) return false;
		Logger.getGlobal().info("recovered node state" + nodeStateDocument.toJson());
		
		state.setCurrentTerm(nodeStateDocument.getInteger(CURRENT_TERM, 0));
		state.setLastVotedTerm(nodeStateDocument.getInteger(LAST_VOTED_TERM, 0));
		state.setLastApplied(nodeStateDocument.getInteger(LAST_APPLIED, 0));
		
		return true;
	}
	
	/********************************************************************************/
	/* Persist logs in mongodb */
	/********************************************************************************/
	public LogEntry getLastLogEntry() {
		try {
			Document lastLogDocument = (Document)logCollection.find().sort(new BasicDBObject("_id",-1)).first();
			return mapDocumentToLogEntry(lastLogDocument);
		} catch(Exception e) {
			e.printStackTrace();
			return null;
		}
	}
	
	public LogEntry getLogEntry(int logIndex) {
		try {
			Bson filter = Filters.eq(LOG_INDEX, logIndex);
			Document logDocument = (Document)logCollection.find(filter).first();
			return mapDocumentToLogEntry(logDocument);
		} catch(Exception e) {
			e.printStackTrace();
			return null;
		}
	}
	
	private LogEntry mapDocumentToLogEntry(Document lastLogDocument) {
		if (lastLogDocument == null) return null;
		
		LogEntry.Builder builder = 
				LogEntry
				.newBuilder()
				.setTerm(lastLogDocument.getInteger(LOG_TERM))
				.setIndex(lastLogDocument.getInteger(LOG_INDEX));
		
		int type = lastLogDocument.getInteger(LOG_PAYLOAD_TYPE);
		Binary payload = (Binary)lastLogDocument.get(LOG_PAYLOAD);
		
		try {
			if (type == PAYLOAD_TYPE_MESSAGE) {
				MessagePayLoad messagePayLoad = MessagePayLoad.parseFrom(payload.getData());
				return builder.setMessagePayload(messagePayLoad).build();
				
			} else if (type == PAYLOAD_TYPE_USER) {
				UserPayload userPayload = UserPayload.parseFrom(payload.getData());
				return builder.setUserPayload(userPayload).build();
				
			} else {
				MessageReadPayload messageReadPayload = MessageReadPayload.parseFrom(payload.getData());
				return builder.setMessageReadPayload(messageReadPayload).build();
			}
			
		} catch(InvalidProtocolBufferException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	private Document mapLogEntryToDocument(LogEntry entry) {
		if (entry == null) return null;
		Document document = new Document();
		document.put(LOG_TERM, entry.getTerm());
		document.put(LOG_INDEX, entry.getIndex());
		document.put(LOG_PAYLOAD_TYPE, getLogType(entry));
		
		if (entry.hasUserPayload()) {
			document.put(LOG_PAYLOAD, new Binary(entry.getUserPayload().toByteArray()));
		} else if (entry.hasMessagePayload()) {
			document.put(LOG_PAYLOAD, new Binary(entry.getMessagePayload().toByteArray()));
		} else {
			document.put(LOG_PAYLOAD, new Binary(entry.getMessageReadPayload().toByteArray()));
		}
		
		return document;
	}
	
	public List<LogEntry> getLogEntriesFromIndex(int fromIndex) {
		List<LogEntry> entries = new ArrayList<LogEntry>();
		
		try {
			FindIterable<Document> documents = logCollection.find(Filters.gte(LOG_INDEX, fromIndex));
			Iterator<Document> iterator = documents.iterator();
			while (iterator.hasNext()) {
				Document document = iterator.next();
				LogEntry entry = mapDocumentToLogEntry(document);
				entries.add(entry);
			}
			
		} catch (Exception ex) {
			ex.printStackTrace();
			return null;
		} 
		
		return entries;
	}
	
	public void deleteLogEntries(int fromIndex) {
		try {
			logCollection.deleteMany(Filters.gte(LOG_INDEX, fromIndex));
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}
	
	public void commitLogEntries(List<LogEntry> entries) {
		Logger.getGlobal().info("going to commit logs, size: " + entries.size());
		try {
			List<Document> documents = new ArrayList<Document>();
			for (LogEntry entry : entries) documents.add(mapLogEntryToDocument(entry));
			if (documents.size() > 0) logCollection.insertMany(documents);
		} catch (Exception ex) {
			ex.printStackTrace();
		} 
	}
	
	
	private int getLogType(LogEntry entry) {
		if (entry.hasUserPayload()) {
			return PAYLOAD_TYPE_USER;
		} else if (entry.hasMessagePayload()) {
			return PAYLOAD_TYPE_MESSAGE;
		} else if (entry.hasMessageReadPayload()) {
			return PAYLOAD_TYPE_MESSAGES_READ;
		} else {
			return -1;
		}
	}
	
	public List<LogEntry> getLogEntriesBetween(int fromIndex, int toIndex) {
		List<LogEntry> entries = new ArrayList<LogEntry>();
		
		try {
			FindIterable<Document> documents = logCollection.find(
					Filters.and(Filters.gt(LOG_INDEX, fromIndex), Filters.lte(LOG_INDEX, toIndex)));
			
			Iterator<Document> iterator = documents.iterator();
			while (iterator.hasNext()) {
				Document document = iterator.next();
				LogEntry entry = mapDocumentToLogEntry(document);
				entries.add(entry);
			}
			
		} catch (Exception ex) {
			ex.printStackTrace();
			return null;
		} 
		
		return entries;
	}
	
}
