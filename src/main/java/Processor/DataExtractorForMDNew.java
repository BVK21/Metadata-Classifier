package Processor;

import Constants.Constants;
import DBConnection.MongoConWithSpark;
import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;
import com.opencsv.CSVWriter;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DataExtractorForMDNew {
    public static File file = new File("/home/bhim/MetaDataClassifier/src/main/resources/VerticalMetaData.csv");
    public static FileWriter outputfile;


    static {
        try {
            outputfile = new FileWriter(file);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static CSVWriter writer = new CSVWriter(outputfile);

    public static void main(String[] args) throws IOException {

        MongoConWithSpark con = new MongoConWithSpark(Constants.DbTypeEnum.SPARK);
        SparkSession s = con.getMongoDbConnection();
        getDataSet(s);
        //getPaperNames(s);


    }


    public static void getDataSet(SparkSession s) {
        JavaSparkContext jsc = new JavaSparkContext(s.sparkContext());
        JavaMongoRDD<Document> rdd = MongoSpark.load(jsc);
        rdd.foreach(rp -> {
            JSONObject rpJson = new JSONObject(rp.toJson());
            if (rpJson.keySet().contains("table") && rpJson.get("table") != null) {
               //getTableMetaHorizData(rpJson.getJSONArray("table"));
               getTableMetaVerticalDataEqualToData(rpJson.getJSONArray("table"));
                //getTableMetaHorizDataEqualToData(rpJson.getJSONArray("table"));
                //getTableDataOnlyLastRow(rpJson.getJSONArray("table"));
                getTableDataOnlyAllRows(rpJson.getJSONArray("table"));
            }
        });


    }

    public static void getPaperNames(SparkSession s){
        JavaSparkContext jsc = new JavaSparkContext(s.sparkContext());
        JavaMongoRDD<Document> rdd = MongoSpark.load(jsc);
        AtomicInteger paperCount = new AtomicInteger();
        AtomicInteger counter = new AtomicInteger();
        rdd.foreach(rp -> {
            counter.getAndIncrement();
            JSONObject rpJson = new JSONObject(rp.toJson());
            if((paperCount.get() < 100) && (counter.get() %100 == 0) ) {

                if (rpJson.keySet().contains("paper_name") && rpJson.get("paper_name") != null && rpJson.keySet().contains("table") && rpJson.get("table") != null) {
                    paperCount.getAndIncrement();
                    System.out.println(paperCount + ":" + rpJson.get("paper_name").toString());
                }
            }
        });
    }

    public static void getTableMetaHorizData(JSONArray tableJson) throws Exception {
        if (tableJson == null)
            throw new Exception("Invalid Json");

        for (int i = 0; i < tableJson.length(); i++) {
            List<String> data = new ArrayList<>();
            JSONObject table = new JSONObject(tableJson.get(i).toString());
            StringBuilder sb = new StringBuilder();
            if (table.keySet().contains("metadata_Horiz") && table.getJSONArray("metadata_Horiz") != null) {
                JSONArray metah = new JSONArray(table.getJSONArray("metadata_Horiz"));
                sb.append("[");
                int cellCount = 0;
                int belowRowCellCount = 0;
                int aboveRowCellCount = 0 ;
                for (int j = 0; j < metah.length(); j++) {
                    JSONObject jsonobject = new JSONObject(metah.getJSONObject(j).toString());
                    if (jsonobject.getInt("level") == 2) {
                        cellCount++;
                        sb.append(jsonobject.getString("text")).append("##@##");
                    } else if (jsonobject.getInt("level") == 1) {
                        aboveRowCellCount++;
                    }
                }
                if (belowRowCellCount == 0) {
                    if (table.keySet().contains("table_data") && table.getJSONArray("table_data") != null) {
                        JSONArray tableData = new JSONArray(table.getJSONArray("table_data"));
                        belowRowCellCount = tableData.get(0).toString().replaceAll("[\\n\t ]", "").replaceAll("\\[", "").replaceAll("\\]", "").split(",").length;
                    }

                }
                if(sb.length() > 5) {
                    sb.setLength(sb.length() - 5);
                    sb.append("]");
                    //Sequence of cells Seperated by Separator
                    data.add(sb.toString().replaceAll("[\\n\t ]", ""));
                    // number of cells
                    data.add(String.valueOf(cellCount));
                    // Label for rows above exits
                    data.add(String.valueOf(1));
                    // Label for rows below exists
                    data.add(String.valueOf(1));
                    //Number of cells above row
                    data.add(String.valueOf(aboveRowCellCount));
                    //number of cells in the below row
                    data.add(String.valueOf(belowRowCellCount));
                    //Label for meta data row
                    data.add(String.valueOf(1));
                    // Row (Tuple) number
                    data.add(String.valueOf(2));
                    // Row number is 2
                    data.add(String.valueOf(1));
                    //Row number is 3
                    data.add(String.valueOf(0));
                    //Row Number is 4
                    data.add(String.valueOf(0));
                    //Row number is 5
                    data.add(String.valueOf(0));
                    // Row number greater than  5
                    data.add(String.valueOf(0));

                    String[] itemsArray = new String[data.size()];
                    itemsArray = data.toArray(itemsArray);

                    writer.writeNext(itemsArray);
                }

            }


        }
    }

    public static void getTableMetaHorizDataEqualToData(JSONArray tableJson) throws Exception {
        if (tableJson == null)
            throw new Exception("Invalid Json");

        for (int i = 0; i < tableJson.length(); i++) {

            JSONObject table = new JSONObject(tableJson.get(i).toString());
            StringBuilder sb = new StringBuilder();
            if (table.keySet().contains("metadata_Horiz") && table.getJSONArray("metadata_Horiz") != null) {
                JSONArray metah = new JSONArray(table.getJSONArray("metadata_Horiz"));
                sb.append("[");
                int cellCount = 0;
                int belowRowCellCount = 0;
                for (int j = 0; j < metah.length(); j++) {
                    JSONObject jsonobject = new JSONObject(metah.getJSONObject(j).toString());
                    if (jsonobject.getInt("level") == 1) {
                        cellCount++;
                        sb.append(jsonobject.getString("text")).append("##@##");
                    } else if (jsonobject.getInt("level") == 2) {
                        belowRowCellCount++;
                    }
                }
                if (belowRowCellCount == 0) {
                    if (table.keySet().contains("table_data") && table.getJSONArray("table_data") != null) {
                        JSONArray tableData = new JSONArray(table.getJSONArray("table_data"));
                        belowRowCellCount = tableData.get(0).toString().replaceAll("[\\n\t ]", "").replaceAll("\\[", "").replaceAll("\\]", "").split(",").length;
                    }

                }
                sb.setLength(sb.length() - 5);
                sb.append("]");
                if (table.keySet().contains("table_data") && table.getJSONArray("table_data") != null) {
                    JSONArray tableData = new JSONArray(table.getJSONArray("table_data"));

                    for (int j = 0; j < tableData.length(); j++) {
                        List<String> data = new ArrayList<>();
                        //Sequence of cells Seperated by Separator
                        data.add(sb.toString().replaceAll("[\\n\t ]", ""));
                        // number of cells
                        data.add(String.valueOf(cellCount));
                        // Label for rows above exits
                        data.add(String.valueOf(0));
                        // Label for rows below exists
                        data.add(String.valueOf(1));
                        //Number of cells above row
                        data.add(String.valueOf(0));
                        //number of cells in the below row
                        data.add(String.valueOf(belowRowCellCount));
                        //Label for meta data row
                        data.add(String.valueOf(1));
                        // Row (Tuple) number
                        data.add(String.valueOf(1));
                        // Row number is 2
                        data.add(String.valueOf(0));
                        //Row number is 3
                        data.add(String.valueOf(0));
                        //Row Number is 4
                        data.add(String.valueOf(0));
                        //Row number is 5
                        data.add(String.valueOf(0));
                        // Row number greater than  5
                        data.add(String.valueOf(0));

                        String[] itemsArray = new String[data.size()];
                        itemsArray = data.toArray(itemsArray);

                        writer.writeNext(itemsArray);
                    }
                }
            }


        }
    }


    public static void getTableDataOnlyLastRow(JSONArray tableJson) throws Exception {
        if (tableJson == null)
            throw new Exception("Invalid Json");
        Pattern p = Pattern.compile("([\\[\\(\\d\\./><\\-\\+%\\)\\];\\*\\:]?(and|to)?)*");
        Pattern p1 = Pattern.compile("^\\d*\\.\\d+|\\d+\\.\\d*$");
        Pattern p2 = Pattern.compile("\\d+");
        Pattern p3 = Pattern.compile("\\d+.?\\d+");

        for (int i = 0; i < tableJson.length(); i++) {
            List<String> data = new ArrayList<>();
            JSONObject table = new JSONObject(tableJson.get(i).toString());
            if (table.keySet().contains("table_data") && table.getJSONArray("table_data") != null) {
                JSONArray tableData = new JSONArray(table.getJSONArray("table_data"));
                String dataString = tableData.get(tableData.length() - 1).toString();
                dataString = dataString.replaceAll("\\[", "").replaceAll("\\]", "");
                String[] strArray = dataString.split(",");
                StringBuilder sb = new StringBuilder();
                sb.append("[");
                for (String str : strArray) {
                    String temp = str;
                    temp = temp.replaceAll("\\s", "");
                    temp = temp.replaceAll("\\\\n", "");
                    temp = temp.replaceAll("\"", "");
                    Matcher matcher = p.matcher(temp);
                    Matcher matcher1 = p1.matcher(temp);
                    Matcher matcher2 = p2.matcher(temp);
                    Matcher matcher3 = p3.matcher(temp);
                    if (matcher.matches() || matcher1.matches() || matcher2.matches() || matcher3.matches()) {
                        sb.append("NUM").append("##@##");


                    } else {
                        sb.append(str.replaceAll("\\\\n", "").replaceAll("\"", "")).append("##@##");
                    }
                }
                sb.setLength(sb.length() - 5);
                sb.append("]");
                data.add(sb.toString().replaceAll("[\\n\t ]", ""));
                // number of cells
                data.add(String.valueOf(5));
                // Label for rows above exits
                data.add(String.valueOf(1));
                // Label for rows below exists
                data.add(String.valueOf(0));
                //Number of cells above row
                if (tableData.length() - 2 > 0)
                    data.add(String.valueOf(tableData.get(tableData.length() - 2).toString().replaceAll("\\n", "").replaceAll("\\[", "").replaceAll("\\]", "").split(",").length));
                else
                    data.add(String.valueOf(0));

                //number of cells in the below row
                data.add(String.valueOf(0));
                //Label for meta data row
                data.add(String.valueOf(0));
                // Row (Tuple) number
                data.add(String.valueOf(tableData.length() - 1));
                if (tableData.length() < 5 && tableData.length() == 2)
                    // Row number is 2
                    data.add(String.valueOf(1));
                else
                    data.add(String.valueOf(0));
                //Row number is 3
                if (tableData.length() < 5 && tableData.length() - 1 == 3)
                    data.add(String.valueOf(1));
                else
                    data.add(String.valueOf(0));

                if (tableData.length() < 5 && tableData.length() - 1 == 4)
                    //Row Number is 4
                    data.add(String.valueOf(1));
                else
                    data.add(String.valueOf(0));


                if (tableData.length() < 5 && tableData.length() - 1 == 5)
                    //Row number is 5
                    data.add(String.valueOf(1));
                else
                    data.add(String.valueOf(0));
                // Row number greater than  5
                if (tableData.length() - 1 > 5)
                    data.add(String.valueOf(1));
                else
                    data.add(String.valueOf(0));

                String[] itemsArray = new String[data.size()];
                itemsArray = data.toArray(itemsArray);

                writer.writeNext(itemsArray);

            }
        }
    }

    public static void getDataOnlyLastRowWithOutPreprocessing(JSONArray tableJson) throws Exception {
        if (tableJson == null)
            throw new Exception("Invalid Json");


        for (int i = 0; i < tableJson.length(); i++) {
            List<String> data = new ArrayList<>();
            JSONObject table = new JSONObject(tableJson.get(i).toString());
            if (table.keySet().contains("table_data") && table.getJSONArray("table_data") != null) {
                JSONArray tableData = new JSONArray(table.getJSONArray("table_data"));
                String dataString = tableData.get(tableData.length() - 1).toString();



                data.add(dataString);
                data.add(String.valueOf(5));
                data.add(String.valueOf(1));
                data.add(String.valueOf(0));
                if (tableData.length() - 2 > 0)
                    data.add(String.valueOf(tableData.get(tableData.length() - 2).toString().replaceAll("\\n", "").replaceAll("\\[", "").replaceAll("\\]", "").split(",").length));
                else
                    data.add(String.valueOf(0));


                data.add(String.valueOf(0));

                data.add(String.valueOf(0));

                data.add(String.valueOf(tableData.length() - 1));
                if (tableData.length() < 5 && tableData.length() == 2)

                    data.add(String.valueOf(1));
                else
                    data.add(String.valueOf(0));

                if (tableData.length() < 5 && tableData.length() - 1 == 3)
                    data.add(String.valueOf(1));
                else
                    data.add(String.valueOf(0));

                if (tableData.length() < 5 && tableData.length() - 1 == 4)

                    data.add(String.valueOf(1));
                else
                    data.add(String.valueOf(0));


                if (tableData.length() < 5 && tableData.length() - 1 == 5)

                    data.add(String.valueOf(1));
                else
                    data.add(String.valueOf(0));

                if (tableData.length() - 1 > 5)
                    data.add(String.valueOf(1));
                else
                    data.add(String.valueOf(0));

                String[] itemsArray = new String[data.size()];
                itemsArray = data.toArray(itemsArray);

                writer.writeNext(itemsArray);

            }
        }

    }

    public static void getTableDataOnlyAllRows(JSONArray tableJson) throws Exception {
        if (tableJson == null)
            throw new Exception("Invalid Json");
        Pattern p = Pattern.compile("([\\[\\(\\d\\./><\\-\\+%\\)\\];\\*\\:]?(and|to)?)*");
        Pattern p1 = Pattern.compile("^\\d*\\.\\d+|\\d+\\.\\d*$");
        Pattern p2 = Pattern.compile("\\d+");
        Pattern p3 = Pattern.compile("\\d+.?\\d+");

        for (int i = 0; i < tableJson.length(); i++) {
            JSONObject table = new JSONObject(tableJson.get(i).toString());
            if (table.keySet().contains("table_data") && table.getJSONArray("table_data") != null) {
                JSONArray tableData = new JSONArray(table.getJSONArray("table_data"));
                for (int j = 0; j < tableData.length(); j++) {
                    List<String> data = new ArrayList<>();
                    String dataString = tableData.get(j).toString();
                    dataString = dataString.replaceAll("\\[", "").replaceAll("\\]", "");
                    String[] strArray = dataString.split(",");
                    StringBuilder sb = new StringBuilder();
                    sb.append("[");
                    for (String str : strArray) {
                        String temp = str;
                        temp = temp.replaceAll("\\s", "");
                        temp = temp.replaceAll("\\\\n", "");
                        temp = temp.replaceAll("\"", "");
                        Matcher matcher = p.matcher(temp);
                        Matcher matcher1 = p1.matcher(temp);
                        Matcher matcher2 = p2.matcher(temp);
                        Matcher matcher3 = p3.matcher(temp);
                        if (matcher.matches() || matcher1.matches() || matcher2.matches() || matcher3.matches()) {
                            sb.append("NUM").append("##@##");


                        } else {
                            sb.append(str.replaceAll("\\\\n", "").replaceAll("\"", "")).append("##@##");
                        }
                    }
                    sb.setLength(sb.length() - 5);
                    sb.append("]");
                    data.add(sb.toString().replaceAll("[\\n\t ]", ""));
                    // number of cells
                    data.add(String.valueOf(tableData.get(j).toString().replaceAll("\\n", "").replaceAll("\\[", "").replaceAll("\\]", "").split(",").length));
                    // Label for rows above exits
                    data.add(String.valueOf(1));
                    // Label for rows below exists
                    if (j == tableData.length() - 1)
                        data.add(String.valueOf(0));
                    else
                        data.add(String.valueOf(1));
                    //Number of cells above row
                    if (j - 1 > 0)
                        data.add(String.valueOf(tableData.get(j - 1).toString().replaceAll("\\n", "").replaceAll("\\[", "").replaceAll("\\]", "").split(",").length));
                    else
                        data.add(String.valueOf(0));

                    if (j + 1 < tableData.length() - 1)
                        //number of cells in the below row
                        data.add(String.valueOf(tableData.get(j + 1).toString().replaceAll("\\n", "").replaceAll("\\[", "").replaceAll("\\]", "").split(",").length));
                    else
                        data.add(String.valueOf(0));
                    //Label for meta data row
                    data.add(String.valueOf(0));
                    // Row (Tuple) number
                    data.add(String.valueOf(j));
                    if (j == 2)
                        // Row number is 2
                        data.add(String.valueOf(1));
                    else
                        data.add(String.valueOf(0));
                    //Row number is 3
                    if (j == 3)
                        data.add(String.valueOf(1));
                    else
                        data.add(String.valueOf(0));

                    if (j == 4)
                        //Row Number is 4
                        data.add(String.valueOf(1));
                    else
                        data.add(String.valueOf(0));


                    if (j == 5)
                        //Row number is 5
                        data.add(String.valueOf(1));
                    else
                        data.add(String.valueOf(0));
                    // Row number greater than  5
                    if (tableData.length() - 1 > 5)
                        data.add(String.valueOf(1));
                    else
                        data.add(String.valueOf(0));

                    String[] itemsArray = new String[data.size()];
                    itemsArray = data.toArray(itemsArray);

                    writer.writeNext(itemsArray);

                }
            }
        }
    }

    public static void getTableMetaVerticalDataEqualToData(JSONArray tableJson) throws Exception {
        if (tableJson == null)
            throw new Exception("Invalid Json");

        for (int i = 0; i < tableJson.length(); i++) {

            JSONObject table = new JSONObject(tableJson.get(i).toString());
            StringBuilder sb = new StringBuilder();
            if (table.keySet().contains("metadata_Vert") && table.getJSONArray("metadata_Vert") != null) {
                JSONArray metaV = new JSONArray(table.getJSONArray("metadata_Vert"));
                sb.append("[");
                int cellCount = 0;
                int belowRowCellCount = 0;
                for (int j = 0; j < metaV.length(); j++) {
                    JSONObject jsonobject = new JSONObject(metaV.getJSONObject(j).toString());
                    if (jsonobject.getInt("level") == 1) {
                        cellCount++;
                        sb.append(jsonobject.getString("text")).append("##@##");
                    } else if (jsonobject.getInt("level") == 2) {
                        belowRowCellCount++;
                    }
                }
                if (belowRowCellCount == 0) {
                    if (table.keySet().contains("table_data") && table.getJSONArray("table_data") != null) {
                        JSONArray tableData = new JSONArray(table.getJSONArray("table_data"));
                        belowRowCellCount = tableData.length();
                    }

                }
                if (sb.length() > 5) {
                    sb.setLength(sb.length() - 5);
                    sb.append("]");

                    if (table.keySet().contains("table_data") && table.getJSONArray("table_data") != null) {
                        JSONArray tableData = new JSONArray(table.getJSONArray("table_data"));

                        for (int j = 0; j < tableData.length(); j++) {
                            List<String> data = new ArrayList<>();
                            //Sequence of cells Seperated by Separator
                            data.add(sb.toString().replaceAll("[\\n\t ]", ""));
                            // number of cells
                            data.add(String.valueOf(cellCount));
                            // Label for rows above exits
                            data.add(String.valueOf(0));
                            // Label for rows below exists
                            data.add(String.valueOf(1));
                            //Number of cells above row
                            data.add(String.valueOf(0));
                            //number of cells in the below row
                            data.add(String.valueOf(belowRowCellCount));
                            //Label for meta data row
                            data.add(String.valueOf(1));
                            // Row (Tuple) number
                            data.add(String.valueOf(1));
                            // Row number is 2
                            data.add(String.valueOf(0));
                            //Row number is 3
                            data.add(String.valueOf(0));
                            //Row Number is 4
                            data.add(String.valueOf(0));
                            //Row number is 5
                            data.add(String.valueOf(0));
                            // Row number greater than  5
                            data.add(String.valueOf(0));

                            String[] itemsArray = new String[data.size()];
                            itemsArray = data.toArray(itemsArray);
                            try {
                                writer.writeNext(itemsArray);
                            } catch (Exception E) {
                                throw new Exception("Error ");
                            }
                        }
                    }

                }
            }
        }
    }


    public static void  getTableAllRowsWithOutPreprocessing(JSONArray tableJson) throws Exception {
        if (tableJson == null)
            throw new Exception("Invalid Json");


        for (int i = 0; i < tableJson.length(); i++) {
            JSONObject table = new JSONObject(tableJson.get(i).toString());
            if (table.keySet().contains("table_data") && table.getJSONArray("table_data") != null) {
                JSONArray tableData = new JSONArray(table.getJSONArray("table_data"));
                for (int j = 0; j < tableData.length(); j++) {
                    List<String> data = new ArrayList<>();
                    String dataString = tableData.get(j).toString();


                    data.add(dataString);

                    data.add(String.valueOf(0));

                    data.add(String.valueOf(1));

                    if (j == tableData.length() - 1)
                        data.add(String.valueOf(0));
                    else
                        data.add(String.valueOf(1));

                    if (j - 1 > 0)
                        data.add(String.valueOf(tableData.get(j - 1).toString().replaceAll("\\n", "").replaceAll("\\[", "").replaceAll("\\]", "").split(",").length));
                    else
                        data.add(String.valueOf(0));

                    if (j + 1 < tableData.length() - 1)

                        data.add(String.valueOf(tableData.get(j + 1).toString().replaceAll("\\n", "").replaceAll("\\[", "").replaceAll("\\]", "").split(",").length));
                    else
                        data.add(String.valueOf(0));

                    data.add(String.valueOf(0));

                    data.add(String.valueOf(j));
                    if (j == 2)

                        data.add(String.valueOf(1));
                    else
                        data.add(String.valueOf(0));

                    if (j == 3)
                        data.add(String.valueOf(1));
                    else
                        data.add(String.valueOf(0));

                    if (j == 4)

                        data.add(String.valueOf(1));
                    else
                        data.add(String.valueOf(0));


                    if (j == 5)

                        data.add(String.valueOf(1));
                    else
                        data.add(String.valueOf(0));

                    if (tableData.length() - 1 > 5)
                        data.add(String.valueOf(1));
                    else
                        data.add(String.valueOf(0));

                    String[] itemsArray = new String[data.size()];
                    itemsArray = data.toArray(itemsArray);

                    writer.writeNext(itemsArray);

                }
            }
        }
    }

    public static void getTermsToFindSimilarity() throws IOException {
        File file=new File("/Users/bhim/BigLabProjects/MetaDataClassifier/src/main/resources/terms");
        FileReader fr=new FileReader(file);
        BufferedReader br=new BufferedReader(fr);
        StringBuffer sb=new StringBuffer();
        String line;
        while((line=br.readLine())!=null)
        {
            System.out.println(line.split(",")[0]);
        }
        fr.close();
    }


    public static void getPerfectTable(){

    }



}
