package com.dch.app.files;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

/**
 * Created by dcherdyntsev on 15.07.2015.
 */
public class BigFilesJoiner {

    private static final Logger logger = LoggerFactory.getLogger(BigFilesJoiner.class);

    public static final int FILES_COUNT = 50;
    public static final char DEFAULT_COLUMNS_DELIMETER = FileKeySplitter.DEFAULT_COLUMNS_DELIMETER;

    private long rowCount = 0L;

    private ConsumeTimer consumeTimer;

    public BigFilesJoiner() {
        consumeTimer = new ConsumeTimer(FileKeySplitter.LOG_TIME, (time)->{
            logger.debug("rows joined: " + rowCount);
        });
    }

    public void splitAndJoin(FileKeySplitter fSplitter, FileKeySplitter rSplitter, String outFile) throws IOException {
        fSplitter.splitByKey();
        rSplitter.splitByKey();

        leftOuterJoin(fSplitter, rSplitter, outFile);
    }

    public void leftOuterJoin(FileKeySplitter fSplitter, FileKeySplitter rSplitter, String outFile) throws IOException {
        logger.debug("join " + fSplitter.getOutDir() + " and " + rSplitter.getOutDir() + " to " + outFile);
        BufferedWriter bwriter = new BufferedWriter(new FileWriter(outFile));
        CsvUtils.writeln(bwriter, fSplitter.getReader().getHeader() + DEFAULT_COLUMNS_DELIMETER + rSplitter.getReader().getHeaderWithoutKey());
        Map<Long, String> map = null;
        for(FileKeySplitter.ReadItem item : fSplitter.getReader().readFilesIterable()) {
            rowCount++;
            consumeTimer.consumeIfNeed();
            if(item.isChangeFile()) {
                if(item.getFileNum() > -1) {
                    map = rSplitter.getReader().getFileHash(item.getFileNum());
                }
                logger.debug("process " + item.getFileNum() + " file, mapNull " + (map == null));
            }
            if(map==null || map.get(item.getKey()) == null || item.getKey()==-1L)
                CsvUtils.writeln(bwriter, item.getLine() + DEFAULT_COLUMNS_DELIMETER + rSplitter.getReader().getEmptyRow());
            else
                CsvUtils.writeln(bwriter, item.getLine() + DEFAULT_COLUMNS_DELIMETER + map.get(item.getKey()));
        }
        logger.debug(rowCount + " rows join");
        bwriter.close();
    }

}
