package Experiments;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TestDriver {
    public static void getTermsToFindSimilarity() throws IOException {
        List<String> data = new ArrayList<>();
        File file = new File("/Users/bhim/BigLabProjects/MetaDataClassifier/src/main/resources/terms");
        FileReader fr=new FileReader(file);
        BufferedReader br=new BufferedReader(fr);
        StringBuffer sb=new StringBuffer();
        String line;
        while((line=br.readLine())!=null)
        {
            line = line.replaceAll("\"","");
            line = line.split("]")[0];
            line = line.replaceAll("\\[","");
            line = line.replaceAll("\\\\n","");
            line = line.replaceAll("\\?","");
            if(line.charAt(0) == ','){
                line = line.substring(1,line.length() -1);
            }
            String[] terms = line.split(",");
            for(int i = 0 ; i < terms.length ; i++){
                String[] temp = terms[i].split(" ");
                for(int k  = 0 ; k < temp.length ; k++){
                    data.add(temp[k].toLowerCase());
                }
            }
        }
        fr.close();

        for(String d : data){
            System.out.println(d);
        }
    }

    public static void main(String[] args) throws IOException {
        getTermsToFindSimilarity();
    }
}
