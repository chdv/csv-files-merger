package com.dch.app.files;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.util.function.Function;

/**
 * Created by dcherdyntsev on 18.07.2015.
 */
public class FileConverter {

    private Path fileIn;
    private Path fileOut;

    public FileConverter(Path fileIn, Path fileOut) {
        this.fileIn = fileIn;
        this.fileOut = fileOut;
    }

    public void convert(Function<String, String> func) throws FileManipulateException  {
        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter(fileOut.toFile()));
            new FileReadIterable(fileIn)
                    .forEach((line) -> {
                        try {
                            writer.write(func.apply(line) + '\n');
                        } catch (IOException e) {
                            throw new FileManipulateException(e);
                        }
                    });
            writer.close();
        } catch (IOException e) {
            throw new FileManipulateException(e);
        }
    }
}
