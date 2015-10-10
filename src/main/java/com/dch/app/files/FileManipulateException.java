package com.dch.app.files;

/**
 * Created by dcherdyntsev on 18.07.2015.
 */
public class FileManipulateException extends RuntimeException {

    public FileManipulateException(String message) {
        super(message);
    }

    public FileManipulateException(String message, Throwable cause) {
        super(message, cause);
    }

    public FileManipulateException(Throwable cause) {
        super(cause);
    }
}