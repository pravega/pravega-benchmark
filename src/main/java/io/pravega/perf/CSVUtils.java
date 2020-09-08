package io.pravega.perf;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class CSVUtils {
    private final static String fileName = "write_data.csv";

    public static boolean addDataToCSV(File file, List<String> dataList) {
        BufferedWriter br=null;
        try {
            br = new BufferedWriter(new FileWriter(file, true));
            for (int i = 0; i < dataList.size(); i++) {
                br.write(dataList.get(i));
                br.write("\r");
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            return false;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        } finally {
            try {
                br.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return true;
    }

    public static void importCSV(String filePath, List<String> dataList) {
        BufferedReader br = null;
        File csv = new File(filePath + fileName);
        if(!csv.exists()) {
            try {
                csv.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        addDataToCSV(csv, dataList);
    }

    public static void main(String[] args) {
        List<String> dataList = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            dataList.add("TEST-DATA-" + i);
        }
        importCSV("./", dataList);
    }
}
