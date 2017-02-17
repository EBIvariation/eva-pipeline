package uk.ac.ebi.eva.pipeline.runner.exceptions;

public class UnrecognizedElement extends Exception {

    public UnrecognizedElement(String arg) {
        super("Element '"+arg+"' not recognized");
    }
}
