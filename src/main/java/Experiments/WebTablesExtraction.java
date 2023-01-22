package Experiments;

import DBConnection.VerticaConnection;
import com.opencsv.CSVWriter;

import java.io.*;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class WebTablesExtraction {

    private static  String mongoIdFileName = "/Users/bhim/BigLabProjects/MetaDataClassifier/src/main/resources/monoIdsFile";

    public static File file = new File("/Users/bhim/BigLabProjects/MetaDataClassifier/src/main/resources/WTTrainingData.csv");
    public static FileWriter outputfile;

    static {
        try {
            outputfile = new FileWriter(file);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static CSVWriter writer = new CSVWriter(outputfile);

    public static void main(String[] args) throws Exception {
        VerticaConnection vc = new VerticaConnection();
        Connection con = vc.getConnection();
        getUniqueTableIds(con);
       getTrainingData(getUniformTrainingData(mongoIdFileName),con);

    }

    public static void getUniqueTableIds( Connection con ) throws Exception {
        Statement st = con.createStatement();
        String mongoIdQuery = getMongoIdQuery();
        ResultSet rs = st.executeQuery(mongoIdQuery);
        wireResultSetContentToFile(rs);
    }

    public static String getMongoIdQuery(){
        StringBuilder st = new StringBuilder();
        st.append("SELECT distinct mongoId from allData.wideTableEng");
        return st.toString();
    }

    public static void wireResultSetContentToFile(ResultSet rs) throws IOException, SQLException {
        File file = new File(mongoIdFileName);
        FileWriter myWriter = new FileWriter(file);
        while (rs.next()){
            myWriter.write(rs.getString("mongoId") + "\r");

        }
        myWriter.close();


    }

    public static List<String> getUniformTrainingData ( String filePath) throws Exception {
        if (filePath == null || filePath.isEmpty())
            throw new Exception("Invalid Input");
        BufferedReader in = new BufferedReader(new FileReader(filePath));
        String str;

        List<String> list = new ArrayList<>();
        int i = 0 ;
        while ((str = in.readLine()) != null) {
            if(i %10000 == 0 ) {
                list.add(str);
            }
            i++;
        }
        return list;
    }

    public static void getTrainingData (List<String> mongoIds, Connection con) throws SQLException {

        if(mongoIds.size() != 0 || con != null){
            for(String str : mongoIds){
                Statement st = con.createStatement();
                ResultSet rs = st.executeQuery(getTableQuery(str));
                getTableData(rs);

            }
        }

    }

    public static String getTableQuery(String mongoId){
        StringBuilder str = new StringBuilder();
        str.append("SELECT * from allData.wideTableEng").append(" Where ").append("mongoId='").append(mongoId).append("'");
        return str.toString();
    }

    public static  void getTableData(ResultSet rs) throws SQLException {
        Map<String,Boolean> result = new LinkedHashMap<>();
        boolean metaFlagFound = false;
        int count  = 1;
        while(rs.next()){
            if(count == 1 && rs.getInt("meta_flag") == 1) {
                metaFlagFound = true;
                result.put(rs.getString("relation"),true);
            }else{
                result.put(rs.getString("relation"),false);
            }
            count++;

        }
        if(metaFlagFound) {
            writeTrainingCsv(result);
        }
    }



    public static void  writeTrainingCsv(Map<String,Boolean> table){

        List<String> keyList = new ArrayList<>(table.keySet());
        if(keyList.size() > 0 ) {
            for(int i = 0; i < keyList.size(); i++) {
                List<String> res = new ArrayList<>();
                //data
                res.add(keyList.get(i));
                // number of cells
                if(keyList.get(i) != null){
                    String[] dataArray = keyList.get(i).split("##@##");
                    res.add(String.valueOf(dataArray.length));
                }else{
                    continue;
                }
                // Label for rows above exits
                if(i == 1)
                    res.add(String.valueOf(0));
                else
                    res.add(String.valueOf(1));

                // Label for rows below exists
                if(i == keyList.size()-1)
                    res.add(String.valueOf(0));
                else
                    res.add(String.valueOf(1));

                //Number of cells above row
                if (i - 1 >= 0 && keyList.get(i - 1)!= null ) {
                    String[] dataArray1 = keyList.get(i - 1).split("##@##");
                    res.add(String.valueOf(dataArray1.length));
                }
                else
                    res.add(String.valueOf(0));

                //number of cells in the below row
                if (i + 1 < keyList.size()-1 && keyList.get(i+1) != null) {
                    String[] dataArray1 = keyList.get(i + 1).split("##@##");
                    res.add(String.valueOf(dataArray1.length));
                }
                else
                    res.add(String.valueOf(0));


                //Label for meta data row
                if(table.get(keyList.get(i))) {
                    res.add(String.valueOf(1));
                }else
                    res.add(String.valueOf(0));

                // Row (Tuple) number
                res.add(String.valueOf(keyList.size()));


                if (i == 2)
                    // Row number is 2
                    res.add(String.valueOf(1));
                else
                    res.add(String.valueOf(0));


                //Row number is 3
                if (i == 3)
                    res.add(String.valueOf(1));
                else
                    res.add(String.valueOf(0));

                if (i == 4)
                    //Row Number is 4
                    res.add(String.valueOf(1));
                else
                    res.add(String.valueOf(0));


                if (i == 5)
                    //Row number is 5
                    res.add(String.valueOf(1));
                else
                    res.add(String.valueOf(0));
                // Row number greater than  5
                if (i > 5)
                    res.add(String.valueOf(1));
                else
                    res.add(String.valueOf(0));

                String[] itemsArray = new String[res.size()];
                itemsArray = res.toArray(itemsArray);

                if(table.get(keyList.get(i))){
                   int  l = keyList.size();
                    while(l != 0){
                        writer.writeNext(itemsArray);
                        l--;
                    }

                }else{
                    writer.writeNext(itemsArray);
                }




                writer.writeNext(itemsArray);
            }
        }


    }


}
