package com.dch.app.files;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Iterator;

/**
 * Created by dcherdyntsev on 18.07.2015.
 */
public class FileReadIterable implements Iterable<String> {

    private static final Logger logger = LoggerFactory.getLogger(FileReadIterable.class);

    private Path filePath;

    public FileReadIterable(Path path) {
        filePath = path;
    }

    @Override
    public Iterator<String> iterator() {
        Iterator<String> result = null;
        try {
            result = createFilesIterator();
        } catch (Exception e) {
            throw new FileManipulateException(e);
        }
        return result;
    }

    private Iterator<String> createFilesIterator() throws IOException {
        return new Iterator<String>() {

            private BufferedReader breader;
            private String nextLine = null;

            {
                breader = new BufferedReader(new FileReader(filePath.toFile()));
                nextLine = breader.readLine();
            }

            @Override
            public boolean hasNext() {
                return nextLine!=null;
            }

            @Override
            public String next() {
                if(nextLine != null) {
                    String old = nextLine;
                    createNext();
                    return old;
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
