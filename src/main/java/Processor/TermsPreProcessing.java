package Processor;


import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class TermsPreProcessing {

    public static File termsFile = new File("/Users/bhim/BigLabProjects/MetaDataClassifier/src/main/resources/termsVaccineTable1.txt");
    public static File cellsFile = new File("/Users/bhim/BigLabProjects/MetaDataClassifier/src/main/resources/cellsVaccineTable1.txt");

    public static FileWriter termsOutputFile;
    public static FileWriter cellsOutputFile;

    static {
        try {
            termsOutputFile = new FileWriter(termsFile);
            cellsOutputFile = new FileWriter(cellsFile);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) {
        JSONParser parser = new JSONParser();
        try {
            Object obj = parser.parse(new FileReader("/Users/bhim/BigLabProjects/MetaDataClassifier/src/main/resources/vaccineTable1.txt"));
            JSONObject jsonObject = (JSONObject) obj;
            JSONArray tablesList = (JSONArray) jsonObject.get("table");
            Iterator<JSONObject> iterator = tablesList.iterator();
            while (iterator.hasNext()) {
                processTerms(iterator.next());
                //processCells(iterator.next());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void  processTerms(JSONObject json) {
        if (json.containsKey("caption") && json.get("caption").toString().contains("Vaccine under development to combat SARS-CoV-2")) {
            System.out.println(json);
            JSONArray array = (JSONArray) json.get("table_data");
            List<String[]> dataList = new ArrayList<>();
            for (int i = 0; i < array.size(); i++) {
                String data = array.get(i).toString();
                data = data.substring(1, data.length() - 1);
                data = data.replaceAll("\"", "");
                String[] dataArray = data.split(",");
                if(dataArray.length > 1 ) {
                    dataList.add(dataArray);
                }
            }

            for(int i = 0 ; i < dataList.get(i).length  ; i++){
                for(int j = 0 ; j < dataList.size() ; j++){
                    System.out.println(dataList.get(j)[i].toLowerCase());
                }
            }


        }
    }


    public static void processCells(JSONObject json){

    }
}

