package com.dch.app.files;

import java.io.File;
import java.nio.file.Path;

/**
 * Created by dcherdyntsev on 16.07.2015.
 */
public class FileKeySorterBuilder {

    private Path path;
    private String key;
    private long filesCount;
    private String outDir;

    public FileKeySorterBuilder() {

    }

    public FileKeySorterBuilder path(Path path) {
        this.path = path;
        return this;
    }

    public FileKeySorterBuilder key(String key) {
        this.key = key;
        return this;
    }

    public FileKeySorterBuilder filesCount(long filesCount) {
        this.filesCount = filesCount;
        return this;
    }

    public FileKeySorterBuilder outDir(String outDir) {
        this.outDir = outDir;
        return this;
    }

    public FileKeySplitter buildSplitter() {
        FileKeySplitter s = new FileKeySplitter();
        s.setFilesCount(filesCount);
        s.setKeyColumnName(key);
        s.setOutDir(outDir);
        s.setPath(path);

        new File(outDir).mkdirs();
        return s;
    }
}
