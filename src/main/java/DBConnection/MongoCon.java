package DBConnection;

import com.mongodb.MongoException;
import com.mongodb.client.*;
import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.List;

public class MongoCon {
    private String userName = "readUser";
    private String passWord = "mongo232";
    private String hostName = "localhost";
    private String maxPoolSize = "20";
    private String weight = "majority";

    private String portNumber = "27017";

    public MongoCon() {
    }



    public String generateConnectionUrl(){
        StringBuilder str = new StringBuilder();
        str.append("mongodb://").append(userName).append(":").append(passWord).append("@").append(hostName);
        str.append(":").append(portNumber).append("/").append("?").append("authSource=test&").append("maxPoolSize=").append("20").append("&w=").append(weight);
        return str.toString();
    }

    public boolean testConnection(){
        try (MongoClient mongoClient = MongoClients.create(generateConnectionUrl())) {
            MongoDatabase database = mongoClient.getDatabase("test");
            try {
                Bson command = new BsonDocument("ping", new BsonInt64(1));
                Document commandResult = database.runCommand(command);
                System.out.println("Connected successfully to server.");
                return true;
            } catch (MongoException me) {
                System.err.println("An error occurred while attempting to run a command: " + me);
                return false;
            }
        }
    }

    public MongoClient getMongoConnection(){
        if(testConnection()){
            MongoClient client = MongoClients.create(generateConnectionUrl());
            return client;
        }else{
            return null;
        }
    }


    public MongoCollection<Document> getCollection(String databaseName ,String collection){
        MongoClient client = getMongoConnection();
        MongoDatabase database = client.getDatabase(databaseName);
        MongoCollection<Document> cord19_7 = database.getCollection(collection);
        return cord19_7;
    }

    public static void main(String[] args) {
        MongoCon con = new MongoCon();
        MongoCollection<Document> collection = con.getCollection("test","cord19_7");
        List<Document> results = new ArrayList<>();
        FindIterable<Document> iterable = collection.find();
        iterable.into(results);
        System.out.println(results);

    }

}
