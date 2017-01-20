package uk.ac.ebi.eva.utils;

import com.mongodb.ReadPreference;

public class MongoConnection {

    private final String hosts;

    private final String authenticationDatabase;

    private final String user;

    private final String password;

    private final ReadPreference readPreference;

    public MongoConnection(String hosts, String authenticationDatabase, String user, String password,
            String readPreference) {
        this.hosts = hosts;
        this.authenticationDatabase = authenticationDatabase;
        this.user = user;
        this.password = password;
        this.readPreference = ReadPreference.valueOf(readPreference);
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

    public ReadPreference getReadPreference() {
        return readPreference;
    }

    public String getReadPreferenceName() {
        return readPreference.getName();
    }
}
