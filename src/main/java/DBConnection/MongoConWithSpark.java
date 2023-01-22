package DBConnection;

import Constants.Constants;
import org.apache.spark.sql.SparkSession;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class MongoConWithSpark {
    private Constants.DbTypeEnum dbType;
    private Constants.ConnectionStatus status;

    public MongoConWithSpark(Constants.DbTypeEnum dbType) {
        this.dbType = dbType;
        this.status = Constants.ConnectionStatus.NOT_CONNECTED;
    }

    public Constants.DbTypeEnum getDbType() {
        return dbType;
    }

    public void setDbType(Constants.DbTypeEnum dbType) {
        this.dbType = dbType;
    }

    public Constants.ConnectionStatus getStatus() {
        return status;
    }

    public void setStatus(Constants.ConnectionStatus status) {
        this.status = status;
    }

    public Connection getVerticaDbConnection(){
        Connection con;
        try {
            con = DriverManager.getConnection(Constants.getDbUrlForConnection(dbType),Constants.getCredentials());
            setStatus(Constants.ConnectionStatus.CONNECTED);
            return con;
        } catch (SQLException e) {
            setStatus(Constants.ConnectionStatus.NOT_CONNECTED);
            System.out.println("Unable To Connect to Vertica DB please Check the network connections , User Credentials ");
            throw new RuntimeException(e);
        }
    }

    public SparkSession getMongoDbConnection(){
        try{
            SparkSession ss = SparkSession.builder()
                    .master("local")
                    .appName("MongoConnector")
                    .config("spark.mongodb.input.uri", Constants.getDbUrlForConnection(dbType) + "." +Constants.getMongodbCollection())
                    .config("spark.mongodb.output.uri", Constants.getDbUrlForConnection(dbType) + "." +Constants.getMongodbCollection())
                    .config("spark.driver.maxResultSize","8g")
                    .getOrCreate();

            setStatus(Constants.ConnectionStatus.CONNECTED);
            return ss;
        }
        catch (Exception e){
            setStatus(Constants.ConnectionStatus.NOT_CONNECTED);
            System.out.println("Unable To Connect to Mongo DB please Check the network connections , User Credentials ");
            throw new RuntimeException(e);
        }
    }
}
