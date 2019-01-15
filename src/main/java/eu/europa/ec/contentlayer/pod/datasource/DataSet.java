package eu.europa.ec.contentlayer.pod.datasource;

public class DataSet {
    protected String URI;
    protected String Name;
    protected boolean isAuthoretative;

    public DataSet(String URI, String name, boolean isAuthoretative) {
        this.URI = URI;
        Name = name;
        this.isAuthoretative = isAuthoretative;
    }
}
