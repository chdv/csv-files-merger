package com.dch.app.files;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Created by dcherdyntsev on 18.07.2015.
 */
public class CsvReadIterable implements Iterable<Map<String,String>> {

    private static final Logger logger = LoggerFactory.getLogger(FileReadIterable.class);

    private Path filePath;

    public CsvReadIterable(Path path) {
        filePath = path;
    }

    @Override
    public Iterator<Map<String,String>> iterator() {
        Iterator<Map<String,String>> result = null;
        try {
            result = createFilesIterator();
        } catch (Exception e) {
            throw new FileManipulateException(e);
        }
        return result;
    }

    private Iterator<Map<String,String>> createFilesIterator() throws IOException {
        return new Iterator<Map<String,String>>() {

            private BufferedReader breader;
            private String nextLine = null;
            private String[] header;
//            private int indx = 0;

            {
                breader = new BufferedReader(new FileReader(filePath.toFile()));
                nextLine = breader.readLine();
                header = CsvUtils.truncateQuotes(CsvUtils.split(nextLine, CsvUtils.DEFAULT_COLUMNS_DELIMETER));
                nextLine = breader.readLine();
            }

            @Override
            public boolean hasNext() {
                return nextLine!=null; //&& indx++ < 3;
            }

            @Override
            public Map<String,String> next() {
                if(nextLine != null) {
                    Map<String,String> result = new LinkedHashMap<>();
                    String[] values = CsvUtils.truncateQuotes(CsvUtils.split(nextLine, CsvUtils.DEFAULT_COLUMNS_DELIMETER));
                    for(int i=0; i<header.length; i++) {
                        String v = CsvUtils.trim2null(values[i]);
                        if(v != null) {
                            result.put(header[i], values[i]);
                        }
                    }
                    createNext();
                    return result;
                } else {
                    return null;
                }
            }

            private void createNext() {
                try {
                    nextLine = breader.readLine();
                    if (nextLine == null) {
                        breader.close();
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

        };
    }
}
