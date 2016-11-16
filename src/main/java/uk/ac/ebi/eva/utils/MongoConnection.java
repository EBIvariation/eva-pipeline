package uk.ac.ebi.eva.utils;

public class MongoConnection {

    public String hosts;

    private String authenticationDatabase;

    private String user;

    private String password;

    private String readPreference;

    public String getReadPreference() {
        return readPreference;
    }

    public void setReadPreference(String readPreference) {
        this.readPreference = readPreference;
    }

    public String getHosts() {
        return hosts;
    }

    public void setHosts(String hosts) {
        this.hosts = hosts;
    }

    public String getAuthenticationDatabase() {
        return authenticationDatabase;
    }

    public void setAuthenticationDatabase(String authenticationDatabase) {
        this.authenticationDatabase = authenticationDatabase;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }
}
