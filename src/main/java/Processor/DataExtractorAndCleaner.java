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

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class DataExtractorAndCleaner {

    public static File file = new File("/Users/bhim/BigLabProjects/MetaDataClassifier/src/main/resources/TrainingData.csv");
    public static FileWriter outputfile;

    static {
        try {
            outputfile = new FileWriter(file);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static CSVWriter writer = new CSVWriter(outputfile);


    public static void main(String[] args) {

        MongoConWithSpark con = new MongoConWithSpark(Constants.DbTypeEnum.SPARK);
        SparkSession s = con.getMongoDbConnection();
        getDataSet(s);

    }


    public static void getDataSet(SparkSession s) {
        JavaSparkContext jsc = new JavaSparkContext(s.sparkContext());
        JavaMongoRDD<Document> rdd = MongoSpark.load(jsc);
        rdd.foreach(rp -> {
            JSONObject rpJson = new JSONObject(rp.toJson());
            if (rpJson.keySet().contains("table") && rpJson.get("table") != null) {
                getTableMetaHorizData(rpJson.getJSONArray("table"));
                getTableDataSample2(rpJson.getJSONArray("table"));
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

                String[] itemsArray = new String[data.size()];
                itemsArray = data.toArray(itemsArray);

                writer.writeNext(itemsArray);

            }


        }
    }

    public static void getTableData(JSONArray tableJson) throws Exception {
        if (tableJson == null)
            throw new Exception("Invalid Json");

        for (int i = 0; i < tableJson.length(); i++) {
            List<String> data = new ArrayList<>();
            JSONObject table = new JSONObject(tableJson.get(i).toString());
            if (table.keySet().contains("table_data") && table.getJSONArray("table_data") != null) {
                JSONArray tableData = new JSONArray(table.getJSONArray("table_data"));

                if (tableData.length() >= 3) {
                    //Sequence of cells Seperated by Separator
                    String dataString = tableData.get(tableData.length()-2).toString();
                    dataString = dataString.replaceAll("\\[", "").replaceAll("\\]","");
                    dataString = dataString.replaceAll("[(){}]","");
                    dataString = dataString.replaceAll(",", "##@##").replaceAll("\"", "").replaceAll("\\\\n","");
                    dataString = dataString.replaceAll("\\n","");
                    dataString = dataString.replaceAll("\\.","");
                    dataString = dataString.replaceAll("\\d+","NUM");
                    data.add("[" + dataString +"]");
                    // number of cells
                    data.add(String.valueOf(tableData.get(tableData.length() - 2).toString().replaceAll("\\n", "").replaceAll("\\[", "").replaceAll("\\]", "").split(",").length));
                    //Label for row above exists
                    data.add(String.valueOf(1));
                    //Label for row below Exists
                    data.add(String.valueOf(1));
                    //Number of cells above row
                    data.add(String.valueOf(tableData.get(tableData.length() - 3).toString().replaceAll("\\n", "").replaceAll("\\[", "").replaceAll("\\]", "").split(",").length));
                    //number of cells in the below row
                    data.add(String.valueOf(tableData.get(tableData.length() - 1).toString().replaceAll("\\n", "").replaceAll("\\[", "").replaceAll("\\]", "").split(",").length));


                    //Label for meta data row
                    data.add(String.valueOf(0));
                    String[] itemsArray = new String[data.size()];
                    itemsArray = data.toArray(itemsArray);

                    writer.writeNext(itemsArray);

                } else if (tableData.length() == 1) {
                    //Sequence of cells Seperated by Separator
                    data.add(tableData.get(tableData.length() - 1).toString().replaceAll("\\n", "").replace(",", "##@##"));
                    // number of cells
                    data.add(String.valueOf(tableData.get(tableData.length() - 1).toString().replaceAll("\\n", "").replaceAll("\\[", "").replaceAll("\\]", "").split(",").length));
                    //Label for row above exists
                    data.add(String.valueOf(1));

                    //Label for row below Exists
                    data.add(String.valueOf(0));


                    //Number of cells above row
                    if (table.keySet().contains("metadata_Horiz") && table.getJSONArray("metadata_Horiz") != null) {
                        JSONArray metah = new JSONArray(table.getJSONArray("metadata_Horiz"));
                        int levelOneCount = 0;
                        int levelTwoCount = 0;
                        for (int j = 0; j < metah.length(); j++) {
                            JSONObject jsonobject = new JSONObject(metah.getJSONObject(j).toString());
                            if (jsonobject.getInt("level") == 1) {
                                levelOneCount++;
                            } else {
                                levelTwoCount++;
                            }
                        }
                        if (levelTwoCount > 0)
                            data.add(String.valueOf(levelTwoCount));
                        else
                            data.add(String.valueOf(levelOneCount));

                        //number of cells in the below row
                        data.add(String.valueOf(0));



                        //Label for meta data row
                        data.add(String.valueOf(0));
                        

                        String[] itemsArray = new String[data.size()];
                        itemsArray = data.toArray(itemsArray);

                        writer.writeNext(itemsArray);



                } else {
                    //Sequence of cells Seperated by Separator
                    data.add(tableData.get(tableData.length() - 2).toString().replaceAll("\\n", "").replace(",", "##@##"));
                    // number of cells
                    data.add(String.valueOf(tableData.get(tableData.length() - 2).toString().replaceAll("[\\n\t ]", "").replaceAll("\\[", "").replaceAll("\\]", "").split(",").length));
                    //Label for row above exists
                        data.add(String.valueOf(1));

                        //Label for row below Exists
                        data.add(String.valueOf(1));

                    //Number of cells above row
                    if (table.keySet().contains("metadata_Horiz") && table.getJSONArray("metadata_Horiz") != null) {
                        JSONArray metah = new JSONArray(table.getJSONArray("metadata_Horiz"));
                        int levelOneCount = 0;
                        int levelTwoCount = 0;
                        for (int j = 0; j < metah.length(); j++) {
                            JSONObject jsonobject = new JSONObject(metah.getJSONObject(j).toString());
                            if (jsonobject.getInt("level") == 1) {
                                levelOneCount++;
                            } else {
                                levelTwoCount++;
                            }
                        }
                        if (levelTwoCount > 0)
                            data.add(String.valueOf(levelTwoCount));
                        else
                            data.add(String.valueOf(levelOneCount));

                        //number of cells in the below row
                        data.add(String.valueOf(tableData.get(tableData.length() - 1).toString().replaceAll("[\\n\t ]", "").replaceAll("\\[", "").replaceAll("\\]", "").split(",").length));


                    }
                    //Label for meta data row
                    data.add(String.valueOf(0));

                    String[] itemsArray = new String[data.size()];
                    itemsArray = data.toArray(itemsArray);

                    writer.writeNext(itemsArray);

                }
            }
        }
        }
    }

    public static void getTableDataSample1(JSONArray tableJson) throws Exception {
        if (tableJson == null)
            throw new Exception("Invalid Json");

        for (int i = 0; i < tableJson.length(); i++) {
            List<String> data = new ArrayList<>();
            JSONObject table = new JSONObject(tableJson.get(i).toString());
            if (table.keySet().contains("table_data") && table.getJSONArray("table_data") != null) {
                JSONArray tableData = new JSONArray(table.getJSONArray("table_data"));


                    //Sequence of cells Seperated by Separator
                    String dataString = tableData.get(tableData.length()-1).toString();
                    dataString = dataString.replaceAll("\\[", "").replaceAll("\\]","");
                    dataString = dataString.replaceAll("[(){}]","");
                    dataString = dataString.replaceAll(",", "##@##").replaceAll("\"", "").replaceAll("\\\\n","");
                    dataString = dataString.replaceAll("\\n","");
                    dataString = dataString.replaceAll("\\.","");
                    dataString = dataString.replaceAll("\\d+","NUM");
                    data.add("[" + dataString +"]");
                    // number of cells
                    data.add(String.valueOf(tableData.get(tableData.length() - 1).toString().replaceAll("\\n", "").replaceAll("\\[", "").replaceAll("\\]", "").split(",").length));
                    //Label for row above exists
                    data.add(String.valueOf(1));
                    //Label for row below Exists
                    data.add(String.valueOf(0));
                    //Number of cells above row
                    if(tableData.length() - 1 > 0) {
                        data.add(String.valueOf(tableData.get(tableData.length() - 2).toString().replaceAll("\\n", "").replaceAll("\\[", "").replaceAll("\\]", "").split(",").length));
                    }
                    else{
                        data.add(String.valueOf(0));
                    }

                    //number of cells in the below row
                    data.add(String.valueOf(0));


                    //Label for meta data row
                    data.add(String.valueOf(0));
                    String[] itemsArray = new String[data.size()];
                    itemsArray = data.toArray(itemsArray);

                    writer.writeNext(itemsArray);




                  }
                }
            }

    public static void getTableDataSample2(JSONArray tableJson) throws Exception {
        if (tableJson == null)
            throw new Exception("Invalid Json");

        for (int i = 0; i < tableJson.length(); i++) {
            List<String> data = new ArrayList<>();
            JSONObject table = new JSONObject(tableJson.get(i).toString());
            if (table.keySet().contains("table_data") && table.getJSONArray("table_data") != null) {
                JSONArray tableData = new JSONArray(table.getJSONArray("table_data"));

                for(int j = 0 ; j < tableData.length() -1  ; j++) {
                    //Sequence of cells Seperated by Separator
                    String dataString = tableData.get(j).toString();
                    dataString = dataString.replaceAll("\\[", "").replaceAll("\\]", "");
                    dataString = dataString.replaceAll("[(){}]", "");
                    dataString = dataString.replaceAll(",", "##@##").replaceAll("\"", "").replaceAll("\\\\n", "");
                    dataString = dataString.replaceAll("\\n", "");
                    dataString = dataString.replaceAll("\\.", "");
                    dataString = dataString.replaceAll("\\d+", "NUM");
                    data.add("[" + dataString + "]");
                    // number of cells
                    data.add(String.valueOf(tableData.get(j).toString().replaceAll("\\n", "").replaceAll("\\[", "").replaceAll("\\]", "").split(",").length));
                    //Label for row above exists
                    data.add(String.valueOf(1));
                    //Label for row below Exists
                    data.add(String.valueOf(0));
                    //Number of cells above row
                    if(j - 1 > 0)
                    data.add(String.valueOf(tableData.get(j -1).toString().replaceAll("\\n", "").replaceAll("\\[", "").replaceAll("\\]", "").split(",").length));
                    else
                    data.add(String.valueOf(0));
                    //number of cells in the below row
                    if(j+1 < table.length())
                    data.add(String.valueOf(tableData.get(j + 1).toString().replaceAll("\\n", "").replaceAll("\\[", "").replaceAll("\\]", "").split(",").length));
                    else
                        data.add(String.valueOf(0));


                    //Label for meta data row
                    data.add(String.valueOf(0));
                    String[] itemsArray = new String[data.size()];
                    itemsArray = data.toArray(itemsArray);

                    writer.writeNext(itemsArray);

                }


            }
        }
    }
}

