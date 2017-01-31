package uk.ac.ebi.eva.pipeline.parameters;

import com.mongodb.ReadPreference;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class MongoConnection {

    @Value("${" + JobParametersNames.CONFIG_DB_HOSTS + ":#{null}}")
    private String hosts;

    @Value("${" + JobParametersNames.CONFIG_DB_AUTHENTICATIONDB + ":#{null}}")
    private String authenticationDatabase;

    @Value("${" + JobParametersNames.CONFIG_DB_USER + ":#{null}}")
    private String user;

    @Value("${" + JobParametersNames.CONFIG_DB_PASSWORD + ":#{null}}")
    private String password;

    @Value("${" + JobParametersNames.CONFIG_DB_READPREFERENCE + ":#{null}}")
    private String readPreference;

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

    public String getReadPreferenceName() {
        return readPreference;
    }

    public ReadPreference getReadPreference() {
        return ReadPreference.valueOf(readPreference);
    }
}
