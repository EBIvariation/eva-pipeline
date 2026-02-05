package uk.ac.ebi.eva.pipeline.runner.exceptions;

public class FileFormatException extends Exception {
    public FileFormatException(String msg) {
        super(msg);
    }

    public FileFormatException(Exception e) {
        super(e);
    }

    public FileFormatException(String msg, Exception e) {
        super(msg, e);
    }
}