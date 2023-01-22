package Processor;

import com.opencsv.*;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

public class TrainingTestGenerator {
    public static File inputFile = new File("/Users/bhim/BigLabProjects/MetaDataClassifier/src/main/resources/TrainingData.csv");
    public static File trainingFile = new File("/Users/bhim/BigLabProjects/MetaDataClassifier/src/main/resources/TrainData.csv");
    public static File testingFile = new File("/Users/bhim/BigLabProjects/MetaDataClassifier/src/main/resources/TestData.csv");
    public  static FileWriter trainFile;
    public static FileWriter testFile;

    static {
        try {
            trainFile = new FileWriter(trainingFile);
            testFile = new FileWriter(testingFile);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public TrainingTestGenerator() {

    }

    public  static CSVWriter trainWriter = new CSVWriter(trainFile);
    public  static CSVWriter testWriter = new CSVWriter(testFile);

    public static void main(String[] args) {
        try {
            FileReader filereader = new FileReader(inputFile);
            CSVReader csvReader = new CSVReaderBuilder(filereader)
                    .withSkipLines(1)
                    .build();

            List<String[]> allData = csvReader.readAll();

            for (int i = 0; i < allData.size(); i++) {
                if (i % 10 == 0) {
                    testWriter.writeNext(allData.get(i));
                } else {
                    trainWriter.writeNext(allData.get(i));
                }
            }
        }
        catch (Exception e) {
                e.printStackTrace();
            }

    }
    }


