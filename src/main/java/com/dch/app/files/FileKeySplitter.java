package com.dch.app.files;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Created by dcherdyntsev on 15.07.2015.
 */
public class FileKeySplitter {

    private static final Logger logger = LoggerFactory.getLogger(FileKeySplitter.class);

    public static final String HEADER_FILE = "header";
    public static final char DEFAULT_COLUMNS_DELIMETER = ';';
    public static final long LOG_TIME = TimeUnit.MINUTES.toMillis(10);

    private Path path;
    private String keyColumnName;
    private long filesCount;
    private String outDir;
    private Map<Long, Writer> writers = new HashMap<>();

    private ConsumeTimer consumeTimer;

    private long rowCount = 0L;

    public FileKeySplitter() {
        consumeTimer = new ConsumeTimer(LOG_TIME, (time)->{
            logger.debug("rows split: " + rowCount);
        });
    }

    public Path getPath() {
        return path;
    }

    public String getKeyColumnName() {
        return keyColumnName;
    }

    public long getFilesCount() {
        return filesCount;
    }

    public String getOutDir() {
        return outDir;
    }

    public void setPath(Path path) {
        this.path = path;
    }

    public void setKeyColumnName(String keyColumnName) {
        this.keyColumnName = keyColumnName;
    }

    public void setFilesCount(long filesCount) {
        this.filesCount = filesCount;
    }

    public void setOutDir(String outDir) {
        this.outDir = outDir;
    }

    public void process() throws IOException {
        splitByKey();
    }

    public void splitByKey()  throws IOException {
        logger.debug("split " + path.toFile() + " by " + keyColumnName + " to " + outDir);
        BufferedReader breader = new BufferedReader(new FileReader(path.toFile()));
        String line = breader.readLine();
        writeHeader(line);
        int columnIndx = CsvUtils.getIndxByName(CsvUtils.truncateQuotes(CsvUtils.split(line, DEFAULT_COLUMNS_DELIMETER)), keyColumnName);
        while ((line = breader.readLine()) != null) {
            rowCount++;
            consumeTimer.consumeIfNeed();
            Long longValue = getLongKeyValue(line, columnIndx);
            long fileIndex = CsvUtils.getBucketIndex(filesCount, longValue);
            Writer writer = getWriterByIndx(fileIndex);
            CsvUtils.writeln(writer, line);
        }
        logger.debug(rowCount + " rows split");
        breader.close();
        closeWriters();
    }

    private void writeHeader(String header) throws IOException {
        BufferedWriter bwriter = new BufferedWriter(new FileWriter(outDir + HEADER_FILE));
        CsvUtils.writeln(bwriter, header);
        bwriter.close();
    }

    private Writer getWriterByIndx(long indx) throws IOException {
        Writer result = writers.get(indx);
        if(writers.get(indx) == null) {
            result = new BufferedWriter(new FileWriter(outDir + indx));
            writers.put(indx, result);
        }
        return result;
    }

    private void closeWriters() throws IOException {
        for(Writer en : writers.values()) {
            en.close();
        }
    }

    private SReader reader = new SReader();

    public SReader getReader() {
        return reader;
    }

    private Long getLongKeyValue(String line, int columnIndx) {
        String[] fields = CsvUtils.split(line, DEFAULT_COLUMNS_DELIMETER);
        String value = "-1";
        try {
            value = CsvUtils.truncateQuotes(fields[columnIndx]);
        } catch(Exception e) {
            logger.error(e.getMessage() + " col " + columnIndx + " fields " + CsvUtils.toString(fields));
        }
        Long longValue = -1L;
        if(value.trim().length() > 0) {
            try {
                longValue = Long.parseLong(value);
            } catch(Exception e) {
                logger.error("col " + columnIndx + " mes " + e.getMessage() + "\n" + line);
            }
        }

        return longValue;
    }

    private String getLineWithoutKey(String line, int columnIndx) {
        String[] fields = CsvUtils.split(line, DEFAULT_COLUMNS_DELIMETER);
        StringBuilder result = new StringBuilder();
        for(int i = 0; i<fields.length; i++) {
            if(i!=columnIndx) {
                result.append(fields[i]);
                result.append(DEFAULT_COLUMNS_DELIMETER);
            }
        }

        return result.substring(0, result.length() - 1);
    }

    public class SReader {

        public String getHeader() throws IOException {
            BufferedReader breader = new BufferedReader(new FileReader(outDir + HEADER_FILE));
            String line = breader.readLine();
            breader.close();
            return line;
        }

        public String getHeaderWithoutKey() throws IOException {
            return getLineWithoutKey(getHeader(), getColumnIndx());
        }

        public List<File> getSplitFiles() {
            List<File> result = new ArrayList<>();
            for(File f : new File(getOutDir()).listFiles()) {
                if(!f.getName().equals(HEADER_FILE) && CsvUtils.isNumeric(f.getName())) {
                    result.add(f);
                }
            }
            Collections.sort(result, (o1, o2) -> Long.compare(Long.parseLong(o1.getName()), Long.parseLong((o2.getName()))));

            return result;
        }

        public Map<Long, String> getFileHash(Long fileIndx) throws IOException {
            Map<Long, String> result = new HashMap<>();
            FileReader fr = null;
            try {
                fr = new FileReader(outDir + fileIndx);
            } catch(Exception e) {
                return null;
            }
            BufferedReader breader = new BufferedReader(fr);
            String line = null;
            int columnIndx = getColumnIndx();
            while ((line = breader.readLine()) != null) {
                Long longValue = getLongKeyValue(line, columnIndx);
                result.put(longValue, getLineWithoutKey(line, columnIndx));
            }
            breader.close();
            return result;
        }

        public Iterable<ReadItem> readFilesIterable() {
            return new Iterable<ReadItem>() {
                @Override
                public Iterator<ReadItem> iterator() {
                    Iterator<ReadItem> it = null;
                    try {
                        it = readFilesIterator();
                    } catch (IOException e) {
                        throw new FileManipulateException(e);
                    }

                    return it;
                }
            };
        }

        private int getColumnIndx() throws IOException {
            return CsvUtils.getIndxByName(CsvUtils.truncateQuotes(CsvUtils.split(getHeader(), DEFAULT_COLUMNS_DELIMETER)), keyColumnName);
        }

        private String emptyRowLine;

        public String getEmptyRow() throws IOException {
            if(emptyRowLine == null) {
                StringBuilder emptyRowLineB = new StringBuilder();
                String[] emptyRow = new String[CsvUtils.split(getHeader(), DEFAULT_COLUMNS_DELIMETER).length];
                for (int i = 0; i < emptyRow.length - 1; i++) {
                    emptyRowLineB.append("\"\";");
                }
                emptyRowLineB.append("\"\"");
                emptyRowLine = emptyRowLineB.toString();
            }
            return emptyRowLine;
        }

        private Iterator<ReadItem> readFilesIterator() throws IOException {
            return new Iterator<ReadItem>() {

                private BufferedReader breader;
                private List<File> splitFiles;
                private int indx = 0;
                private String nextLine = null;
                private boolean changeFile = true;
                private ReadItem item = null;
                private long keyLong = 0;
                private long fileNum = 0;
                private int columnIndx;

                {
                    columnIndx = getColumnIndx();

                    splitFiles = getSplitFiles();
                    if(splitFiles.size() > 0) {
                        breader = new BufferedReader(new FileReader(splitFiles.get(indx)));
                        fileNum = Long.parseLong(splitFiles.get(indx).getName());
                        nextLine = breader.readLine();
                        readNext();
                    }
                }

                @Override
                public boolean hasNext() {
                    return item!=null;
                }

                @Override
                public ReadItem next() {
                    if(item != null) {
                        ReadItem old = item;
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
                            if (indx < splitFiles.size() - 1) {
                                changeFile = true;
                                indx++;
                                breader.close();
                                breader = new BufferedReader(new FileReader(splitFiles.get(indx)));
                                fileNum = Long.parseLong(splitFiles.get(indx).getName());
                                nextLine = breader.readLine();
                                readNext();
                            } else {
                                // THE END
                                breader.close();
                                item = null;
                            }
                        } else {
                            changeFile = false;
                            readNext();
                        }
                    } catch (IOException e) {
                        throw new FileManipulateException(e);
                    }
                }

                private void readNext() throws IOException {
                    keyLong = getLongKeyValue(nextLine, columnIndx);
                    item = new ReadItem(changeFile, nextLine, fileNum, keyLong);
                }

            };
        }
    }

    public static class ReadItem {
        private boolean changeFile = false;
        private String line;
        private long fileNum;
        private long key;

        public ReadItem(boolean changeFile, String line, long fileNum, long key) {
            this.changeFile = changeFile;
            this.line = line;
            this.fileNum = fileNum;
            this.key = key;
        }

        public boolean isChangeFile() {
            return changeFile;
        }

        public String getLine() {
            return line;
        }

        public long getFileNum() {
            return fileNum;
        }

        public long getKey() {
            return key;
        }
    }

    public static void main(String[] args) throws IOException {
        new FileKeySplitter().process();
    }

}
