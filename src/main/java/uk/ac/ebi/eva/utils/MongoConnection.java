package uk.ac.ebi.eva.utils;

public class MongoConnection {

    private String hosts;

    private String authenticationDatabase;

    private String user;

    private String password;

    private String readPreference;

    public MongoConnection() {
        
    }
    
    public MongoConnection(String hosts, String authenticationDatabase, String user, String password,
            String readPreference) {
        this.hosts = hosts;
        this.authenticationDatabase = authenticationDatabase;
        this.user = user;
        this.password = password;
        this.readPreference = readPreference;
    }

    public String getHosts() {
        return hosts;
    }

    public String getAuthenticationDatabase() {
        return authenticationDatabase;
    }

    public String getUser() {
        return user;
    }

    public String getPassword() {
        return password;
    }

    public String getReadPreference() {
        return readPreference;
    }

    public void setReadPreference(String readPreference) {
        this.readPreference = readPreference;
    }

}
