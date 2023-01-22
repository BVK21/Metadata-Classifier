package DBConnection;



import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class VerticaConnection {

    private String userName = "Ritu";
    private String passWord = "fightback";
    private String hostName = "bl2.cs.fsu.edu";
    private String backUpServerNodes = "bl2,bl3";
    private String weight = "majority";
    private String portNumber = "5433";

    private String schemaName = "ls_struct_crawl";



    public VerticaConnection() {
    }

    public String generateConnectionUrl(){
        StringBuilder str = new StringBuilder();
        str.append("jdbc:vertica://").append(hostName).append(":").append(portNumber).append("/").append(schemaName);
        return str.toString();
    }

    public Connection getConnection() throws SQLException {
        Connection conn;
        Properties myProp = new Properties();
        myProp.put("user", userName);
        myProp.put("password", passWord);
        myProp.put("BackupServerNode", backUpServerNodes);
        conn = DriverManager.getConnection(
                generateConnectionUrl(), myProp);
        if(conn.isValid(100)){
            return conn;
        }else{
            throw new SQLException("Unable to connect to Verica db");
        }

    }



}
