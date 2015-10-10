package com.dch.app.files;

import java.io.*;
import java.nio.file.Paths;

/**
 * Created by dcherdyntsev on 17.07.2015.
 */
public class CsvConverter {

    public void process(String file1, String file2) {
        new FileConverter(Paths.get(file1), Paths.get(file2))
                .convert((line) -> {
                    String[] row = CsvUtils.split(line, ',');
                    StringBuilder sb = new StringBuilder();
                    for (String r : row) {
                        sb.append(CsvUtils.QUOTE);
                        sb.append(r);
                        sb.append(CsvUtils.QUOTE);
                        sb.append(FileKeySplitter.DEFAULT_COLUMNS_DELIMETER);
                    }

                    return sb.substring(0, sb.length() - 1);
                });
    }

    public static void main(String[] args) throws IOException {
        String FILE_NAME1 = "C:\\Workspace\\111.csv";
        String FILE_NAME2 = "C:\\Workspace\\222.csv";

        new CsvConverter().process(FILE_NAME1, FILE_NAME2);
    }
}
