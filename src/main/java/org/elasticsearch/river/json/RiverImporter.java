package org.elasticsearch.river.json;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.Map;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.common.io.Closeables;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.util.concurrent.jsr166y.TransferQueue;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.common.xcontent.json.JsonXContent;


public class RiverImporter {

    private static final int TIMEOUT = 120 * 1000;
    private static final ESLogger logger = ESLoggerFactory.getLogger(RiverImporter.class.getName());
    private static final String TIMESTAMP_PARAMETER = "lastUpdate";
    private final TransferQueue<RiverProduct> queue;
    private final String riverUrl;

    public RiverImporter(String riverUrl, TransferQueue<RiverProduct> stream) {
        this.riverUrl = riverUrl;
        this.queue = stream;
    }

    public RiverProductImport executeImport(String lastIndexUpdate) {
        RiverProductImport result = new RiverProductImport();

        InputStream is = null;

        String timestamp = null;
        @SuppressWarnings("unused")
        Token token = null;
        XContentParser parser = null;

        try {
            is = getConnectionInputstream(lastIndexUpdate);

            parser = JsonXContent.jsonXContent.createParser(is);

            String action = null;
            Map<String, Object> product = null;
            String id = null;

            while ((token = parser.nextToken()) != null) {

                if ("timestamp".equals(parser.text())) {
                    token = parser.nextToken();
                    timestamp = parser.text();
                } else if ("action".equals(parser.text())) {
                    token = parser.nextToken();
                    action = parser.text();
                } else if ("id".equals(parser.text())) {
                    token = parser.nextToken();
                    id = parser.text();
                } else if ("product".equals(parser.text())) {
                    token = parser.nextToken();
                    product = parser.map();
                }

                if ("delete".equals(action) && id != null) {
                    queue.add(RiverProduct.delete(id));
                    action = null;
                    id = null;
                    result.exportedProductCount++;
                }

                if ("index".equals(action) && product != null && id != null) {
                    queue.add(RiverProduct.index(id, product));
                    action = null;
                    product = null;
                    id = null;
                    result.exportedProductCount++;
                }
            }

            result.exportTimestamp = timestamp;

        } catch (IOException e) {
            logger.error("Could not get content with lastUpdatedTimestamp [{}]", e, lastIndexUpdate);
        } finally {
            Closeables.closeQuietly(is);
        }

        return result;
    }

    public InputStream getConnectionInputstream(String lastUpdateTimestamp) throws IOException {
        URL url = null;
        HttpURLConnection connection = null;

        if (lastUpdateTimestamp == null) {
            url = new URL(riverUrl);
        } else {
            char delimiter = riverUrl.contains("?") ? '&' : '?';
            url = new URL(riverUrl + delimiter + TIMESTAMP_PARAMETER + "=" + URLEncoder.encode(lastUpdateTimestamp, "UTF-8"));
        }

        connection = (HttpURLConnection) url.openConnection();
        connection.setUseCaches(false);
        connection.setConnectTimeout(TIMEOUT);
        connection.setReadTimeout(TIMEOUT);

        if (connection.getResponseCode() != 200) {
            String message = String.format("River endpoint problem for url %s: Connection response code was %s %s", url, connection.getResponseCode(), connection.getResponseMessage());
            throw new ElasticSearchException(message);
        }

        return connection.getInputStream();
    }

}
