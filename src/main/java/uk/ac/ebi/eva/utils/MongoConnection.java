package uk.ac.ebi.eva.utils;

public class MongoConnection {

    private final String hosts;

    private final String authenticationDatabase;

    private final String user;

    private final String password;

    private final String readPreference;

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

}
