package embl.ebi.variation.eva.utils;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Paths;

public class URLHelper {

    public static URI createUri(String input) throws URISyntaxException {
        URI sourceUri = new URI(input);
        if (sourceUri.getScheme() == null || sourceUri.getScheme().isEmpty()) {
            sourceUri = Paths.get(input).toUri();
        }
        return sourceUri;
    }

}
