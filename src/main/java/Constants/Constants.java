package Constants;

import java.util.Properties;

public class Constants {
    private static final String mongodbHost = "localhost";
    private static final String mongodbName = "test";
    private static final String mongodbCollection = "cord19_7";
    private static final String verticaDbName = "ls_struct_crawl";
    
    public enum DbTypeEnum {
        SPARK(0),
        VERTICA(1),
        UNKNOWN(3);

        public int v;

        DbTypeEnum(int v) {
            this.v = v;
        }

        public static DbTypeEnum qualify(String key) {

            if (key == null || key.isEmpty()) return UNKNOWN;
            switch (key.toLowerCase()) {
                case "spark":
                    return SPARK;
                case "vertica":
                    return VERTICA;
                default:
                    return UNKNOWN;
            }
        }
    }

    public enum ConnectionStatus {
        CONNECTED,NOT_CONNECTED;
    }

    public static String getDbUrlForConnection(DbTypeEnum dbType){
        if(dbType == DbTypeEnum.SPARK)
            return "mongodb://readUser:mongo232@" + mongodbHost + "/" + mongodbName;
        else if(dbType == DbTypeEnum.VERTICA)
            return "jdbc:vertica://" + verticaDbHost + "/" + verticaDbName;
        else
            return "/";
    }

    public static Properties getCredentials(){
        Properties myProp = new Properties();
        myProp.put("user",verticaUserName );
        myProp.put("password", verticaPassWord);
        myProp.put("BackupServerNode", "bl2,bl3");
        return myProp;
    }

    public static String getDbName( DbTypeEnum dbType){
        if(dbType == DbTypeEnum.SPARK)
            return mongodbName;
        else if(dbType == DbTypeEnum.VERTICA)
            return verticaDbName;
        else
            return "NA";
    }

    public static String getMongodbCollection(){
        return mongodbCollection;
    }

}
