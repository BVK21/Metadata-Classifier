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
import java.util.Arrays;
import java.util.List;

public class EmbeddingsGenerator {

    public static File metaHFile = new File("/home/bhim/MetaDataClassifier/src/main/resources/metaH.csv");
    public static File metaVFile = new File("/home/bhim/MetaDataClassifier/src/main/resources/metaV.csv");
    public static File verticalDataFile = new File("/home/bhim/MetaDataClassifier/src/main/resources/verticalDataFile.csv");
    public static File horizontalDataFile = new File("/home/bhim/MetaDataClassifier/src/main/resources/HorizontalDataFile.csv");
    public static FileWriter wMetaHFile;
    public static FileWriter wMetaVFile;
    public static FileWriter wVerticalDataFile;
    public static FileWriter wHorizontalDataFile;


    static {
        try {
            wMetaHFile = new FileWriter(metaHFile);
            wMetaVFile = new FileWriter(metaVFile);
            wVerticalDataFile = new FileWriter(verticalDataFile);
            wHorizontalDataFile = new FileWriter(horizontalDataFile);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static CSVWriter metaHWriter = new CSVWriter(wMetaHFile);
    public static CSVWriter metaVWriter = new CSVWriter(wMetaVFile);
    public static CSVWriter verticalDataWriter = new CSVWriter(wVerticalDataFile);
    public static CSVWriter horizontalDataWriter = new CSVWriter(wHorizontalDataFile);


    public static void main(String[] args) {

        MongoConWithSpark con = new MongoConWithSpark(Constants.DbTypeEnum.SPARK);
        SparkSession s = con.getMongoDbConnection();
        getDataSetForEmbeddings(s);

    }

    public static void getDataSetForEmbeddings(SparkSession s) {
        JavaSparkContext jsc = new JavaSparkContext(s.sparkContext());
        JavaMongoRDD<Document> rdd = MongoSpark.load(jsc);
        rdd.foreach(rp -> {
            JSONObject rpJson = new JSONObject(rp.toJson());
            if (rpJson.keySet().contains("table") && rpJson.get("table") != null) {
                getEmbeddingsForMetaV(rpJson.getJSONArray("table"));
                getEmbeddingsForMetaH(rpJson.getJSONArray("table"));
                try {
                    getData(rpJson.getJSONArray("table"));
                }catch (Exception e){
                    throw new Exception("Invalid Row ");
                }
            }
        });
    }

    public static void getEmbeddingsForMetaH(JSONArray tableJson) throws Exception {
        if (tableJson == null) {
            throw new Exception("Invalid Json");
        }

        for (int i = 0; i < tableJson.length(); i++) {
            List<String> data = new ArrayList<>();
            JSONObject table = new JSONObject(tableJson.get(i).toString());
            StringBuilder sb = new StringBuilder();
            if (table.keySet().contains("metadata_Horiz") && table.getJSONArray("metadata_Horiz") != null) {
                JSONArray metaH = new JSONArray(table.getJSONArray("metadata_Horiz"));
                sb.append("[");
                for (int j = 0; j < metaH.length(); j++) {
                    JSONObject jsonobject = new JSONObject(metaH.getJSONObject(j).toString());
                    if (jsonobject.getInt("level") == 1) {
                        sb.append(jsonobject.getString("text")).append("##@##");
                    }
                }
                sb.setLength(sb.length() - 5);
                sb.append("]");
            }
            data.add(sb.toString().replaceAll("\\n", ""));
            String[] itemsArray = new String[data.size()];
            itemsArray = data.toArray(itemsArray);

            metaHWriter.writeNext(itemsArray);
        }
    }

    public static void getEmbeddingsForMetaV(JSONArray tableJson) throws Exception {
        if (tableJson == null)
            throw new Exception("Invalid Json");
        for (int i = 0; i < tableJson.length(); i++) {
            List<String> data = new ArrayList<>();
            JSONObject table = new JSONObject(tableJson.get(i).toString());
            StringBuilder sb = new StringBuilder();
            if (table.keySet().contains("metadata_Vert") && table.getJSONArray("metadata_Vert") != null) {
                JSONArray metaV = new JSONArray(table.getJSONArray("metadata_Vert"));
                sb.append("[");
                for (int j = 0; j < metaV.length(); j++) {
                    JSONObject jsonobject = new JSONObject(metaV.getJSONObject(j).toString());
                    if (jsonobject.getInt("level") == 1) {
                        sb.append(jsonobject.getString("text")).append("##@##");
                    }
                }
                if(sb.length() > 5) {
                    sb.setLength(sb.length() - 5);
                }
                sb.append("]");
            }
            if(sb.length() > 2) {
                data.add(sb.toString().replaceAll("\\n", ""));
                String[] itemsArray = new String[data.size()];
                itemsArray = data.toArray(itemsArray);

                metaVWriter.writeNext(itemsArray);
            }
        }

    }

    public static void getData(JSONArray tableJson) throws Exception {
        if(tableJson == null)
            throw new Exception("Invalid Json");

        for (int i = 0; i < tableJson.length(); i++) {
            List<String> data = new ArrayList<>();
            JSONObject table = new JSONObject(tableJson.get(i).toString());
            if (table.keySet().contains("table_data") && table.getJSONArray("table_data") != null) {
                JSONArray tableData = new JSONArray(table.getJSONArray("table_data"));
                List<List<String>> data_list = new ArrayList<>();
                for(int j = 0 ; j < tableData.length() ; j++){
                    data_list.add(Arrays.asList(tableData.get(j).toString().replaceAll("\\n","").replaceAll("\\[", "").replaceAll("\\]","").split(",")));
                }
                getVerticalDataFromMatrix(data_list);
                getHorizontalDataFromMatrix(data_list);
            }
        }
    }

    public static void getVerticalDataFromMatrix(List<List<String>> data) throws Exception {
        try {
            if (data == null || data.size() == 0)
                throw new Exception("Invalid Input ");
            if(sameLength(data)) {

                for (int i = 0; i < data.get(0).size(); i++) {
                    List<String> verticalData = new ArrayList<>();
                    for (int j = 0; j < data.size(); j++) {

                        verticalData.add(data.get(j).get(i));
                    }
                    StringBuilder sb = new StringBuilder();
                    sb.append("[");
                    for (String ele : verticalData) {
                        sb.append(ele).append("##@##");
                    }
                    if (sb.length() > 5) {
                        sb.setLength(sb.length() - 5);
                    }
                    sb.append("]");
                    String[] itemsArray = new String[1];
                    itemsArray[0] = sb.toString();
                    verticalDataWriter.writeNext(itemsArray);
                }
            }
        }catch (Exception e){
            throw new Exception("Problem in the input ");
        }


    }

    public static void getHorizontalDataFromMatrix(List<List<String>> data) throws Exception {
        try {
            if (data == null || data.size() == 0)
                throw new Exception("Invalid Input ");
            if(sameLength(data)){
            for (int i = 0; i < data.size(); i++) {
                List<String> horizontalData = new ArrayList<>();
                for (int j = 0; j < data.get(i).size(); j++) {
                    horizontalData.add(data.get(i).get(j));
                }
                StringBuilder sb = new StringBuilder();
                sb.append("[");
                for (String ele : horizontalData) {
                    sb.append(ele).append("##@##");
                }
                if (sb.length() > 5) {
                    sb.setLength(sb.length() - 5);
                }
                sb.append("]");
                String[] itemsArray = new String[1];
                itemsArray[0] = sb.toString();
                horizontalDataWriter.writeNext(itemsArray);
            }
            }
        }catch (Exception e){
            throw new Exception("Problem in the input ");
        }
    }

    public static boolean sameLength(List<List<String>> lists){
        if(lists.get(0) == null){
            return true;
        }
        int len = lists.get(0).size();
        for (List list : lists) {
            if(list.size() != len){
                return false;
            }
        }
        return true;
    }


}




































