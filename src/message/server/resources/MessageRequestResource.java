package message.server.resources;

import org.bson.Document;

import com.mongodb.client.FindIterable;

import message.database.MessageMongoDB;
import message.database.UserMongoDB;
import message.server.config.RoutingConf;
import message.server.raft.MessageBuilder;
import routing.Pipe.Route;

public class MessageRequestResource implements RouteResource {

	public MessageRequestResource() {
		// TODO Auto-generated constructor stub
	}

	@Override
	public String getPath() {
		// TODO Auto-generated method stub
		return "/messages_request";
	}

	@Override
	public Route process(Route msg) {
		
		String receiver = msg.getMessagesRequest().getId();
		FindIterable<Document> documents =MessageMongoDB.getInstance().get(receiver);
		Route response = MessageBuilder.prepareMessageResponse(msg.getMessagesRequest().getType(), documents);
		MessageMongoDB.getInstance().setRead(receiver);
		return response;
	}
	

}
