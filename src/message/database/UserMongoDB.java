package message.database;

import com.google.protobuf.Timestamp;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.UpdateResult;

import message.server.config.NodeConf;
import message.server.raft.RaftNode;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.bson.Document;

import com.mongodb.client.model.Filters;

import org.bson.conversions.Bson;
import routing.Pipe.Route;
import routing.Pipe.User;

public class UserMongoDB {

	static UserMongoDB UserMongoDB = null;
	MongoClient mongoClient = null;
	MongoDatabase database = null;
	MongoCollection<Document> dbCollection = null;
	final static String DB_NAME = "275db";
	final static String COLLECTION_NAME = "User";

//	final static String USER_ID = "userID";
	final static String USER_NAME = "userName";
//	final static String TIMESTAMP = "timestamp";
//	final static String UNIXTIMESTAMP = "UnixTimeStamp";
//	final static String TIMESTAMPONLATESTUPDATE = "TimeStampOnLatestUpdate";

	private UserMongoDB() {
			// TODO Auto-generated constructor stub
			try {
			NodeConf conf = RaftNode.getInstance().getState().getNodeConf();
			mongoClient = new MongoClient("127.0.0.1", conf.getMongoPort());
			database = mongoClient.getDatabase(DB_NAME);
			dbCollection = database.getCollection(COLLECTION_NAME);
			}
			catch(Exception e) {
				e.printStackTrace();
				return;
			}
		}

	/********************************************************************************/
	/* Get Instance of UserMongoSB to ensure single instance!! 					  */
	/********************************************************************************/
	public static UserMongoDB getInstance() {
		if (UserMongoDB == null) {
			UserMongoDB = new UserMongoDB();
		}
		return UserMongoDB;
	}

	/********************************************************************************/
	/* Get results from table 													  */
	/********************************************************************************/
	/*Get result based on User name */
	public FindIterable<Document> get(String key) {
		System.out.println("***UserMongoDB*** fn:get***");
		FindIterable<Document> result = null;
		try {
			result = dbCollection.find(Filters.eq(USER_NAME, key));
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		} finally {

		}
		return result;

	}
	


	/*Get result based on timestamp of user creation */
//
//	public FindIterable<Document> getNewEntries(Date staleTimeStamp) {
//		System.out.println("***UserMongoDB*** fn:getnewEntries***");
//		FindIterable<Document> result = null;
//		try {
//			result = dbCollection.find(Filters.eq(TIMESTAMP, staleTimeStamp));
//		} catch (Exception ex) {
//			ex.printStackTrace();
//			return null;
//		} finally {
//		}
//		return result;
//	}

	/*Get result - all  entries in the table */

	public FindIterable<Document> getAllEntries() {
		System.out.println("***UserMongoDB*** fn:getAllEntries***");
		FindIterable<Document> result = null;
		try {
			result = dbCollection.find();
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
			return null;
		} finally {

		}
		return result;
	}
	
	/********************************************************************************/
	/* Delete from User table							 						  */
	/********************************************************************************/
	public boolean delete(String userName) {
		System.out.println("***MongoDB*** fn:delete***");
		try {
			dbCollection.deleteOne(Filters.eq(USER_NAME, userName));
			return true;
		} catch (Exception ex) {
			ex.printStackTrace();
			return false;
		} finally {
		}

	}
	
	/********************************************************************************/
	/* Store New User data into DB							 					  */
	/********************************************************************************/
	public boolean storeUserMessagetoDB(Route clientMessage) {
		System.out.println("***MongoDB*** fn:storeClientMessagetoDB");
		try {
			Document document = new Document();
			document.append(USER_NAME, clientMessage.getUser().getUname());
			Date date = new Date(Long.parseLong(clientMessage.getUser().getRecentActiveTime().replace(".", "")));
//			document.append(TIMESTAMP, date);
			dbCollection.insertOne(document);

			return true;
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		} finally {
		}
	}
	
	//todo: store all fields
	public boolean commitUser(User user) {
		System.out.println("***MongoDB*** commitUser");
		try {
			Document document = new Document();
			document.append(USER_NAME, user.getUname());

			Bson filter = Filters.eq(USER_NAME, user.getUname());
			UpdateOptions options = new UpdateOptions().upsert(true);
			dbCollection.replaceOne(filter, document, options);

			return true;
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
	}

	public boolean postUser1() {
		System.out.println("***MongoDB*** fn:storeClientMessagetoDB");
		try {
			Document document = new Document();
			document.append(USER_NAME,"rency");

//			document.append(TIMESTAMP, getDate());
			dbCollection.insertOne(document);

			return true;
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		} finally {}
		}
	
//	public Date getDate() {
//	String DATE_FORMAT_NOW = "yyyy-MM-dd";
//	 Date date = new Date();
//	 SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT_NOW);
//	 String stringDate = sdf.format(date );
//     Date date2 = null;
//
//	    try {
//	    	date2 =sdf.parse(stringDate);
//	    } catch(Exception e){
//	    		e.printStackTrace();	   
//}
//        return date2;
//
//	}
		public boolean postUser2() {
			System.out.println("***MongoDB*** fn:storeClientMessagetoDB");
			try {
				Document document = new Document();
				document.append(USER_NAME,"shefali");
//				document.append(TIMESTAMP, getDate());
				dbCollection.insertOne(document);

				return true;
			} catch (Exception e) {
				e.printStackTrace();
				return false;
			} finally {
			}
	}

	/********************************************************************************/
	/* Close DB Connection							 						  */
	/********************************************************************************/
	public boolean closeDatabaseConnection() {
		System.out.println("***MongoDB*** fn:closeDatabaseConnection***");
		try {
			mongoClient.close();
			return true;
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		} finally {
		}
	}

	

}
