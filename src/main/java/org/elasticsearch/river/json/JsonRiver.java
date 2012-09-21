package org.elasticsearch.river.json;

import static org.elasticsearch.client.Requests.*;

import java.util.concurrent.TimeUnit;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.StopWatch;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.jsr166y.LinkedTransferQueue;
import org.elasticsearch.common.util.concurrent.jsr166y.TransferQueue;
import org.elasticsearch.river.*;

public class JsonRiver extends AbstractRiverComponent implements River {

    private final Client client;
    private final String riverIndexName;

    public static String RIVER_URL = "http://localhost:50000/data";
    public static String RIVER_INDEX = "products";
    public static String RIVER_TYPE = "product";
    public static TimeValue RIVER_REFRESH_INTERVAL = TimeValue.timeValueSeconds(30);
    public static int RIVER_MAX_BULK_SIZE = 5000;

    private volatile Thread slurperThread;
    private volatile Thread indexerThread;
    private volatile boolean closed;

    private final TransferQueue<RiverProduct> stream = new LinkedTransferQueue<RiverProduct>();

    @Inject public JsonRiver(RiverName riverName, RiverSettings settings, @RiverIndexName String riverIndexName, Client client) {
        super(riverName, settings);
        this.riverIndexName = riverIndexName;
        this.client = client;
    }

    @Override
    public void start() {
        logger.info("Starting JSON stream river: url [{}], query interval [{}]", RIVER_URL, RIVER_REFRESH_INTERVAL);

        closed = false;
        try {
            slurperThread = EsExecutors.daemonThreadFactory("json_river_slurper").newThread(new Slurper());
            slurperThread.start();
            indexerThread = EsExecutors.daemonThreadFactory("json_river_indexer").newThread(new Indexer());
            indexerThread.start();
        } catch (ElasticSearchException e) {
            logger.error("Error starting indexer and slurper. River is not running", e);
            closed = true;
        }
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        logger.info("closing json stream river");
        slurperThread.interrupt();
        indexerThread.interrupt();
        closed = true;
    }

    private class Slurper implements Runnable {

        private final ESLogger logger = ESLoggerFactory.getLogger(this.getClass().getName());

        @Override
        public void run() {
            RiverImporter importer = new RiverImporter(RIVER_URL, stream);

            while (!closed) {
                logger.debug("Slurper run() started");
                String lastIndexUpdate = getLastUpdatedTimestamp();

                try {
                    RiverProductImport result = importer.executeImport(lastIndexUpdate);

                    if (result.exportTimestamp != null) {
                        storeLastUpdatedTimestamp(result.exportTimestamp);
                    }

                    logger.info("Slurping [{}] documents with timestamp [{}]", result.exportedProductCount, result.exportTimestamp);
                } catch (ElasticSearchException e) {
                    logger.error("Failed to import data from json stream", e);
                }

                try {
                    Thread.sleep(RIVER_REFRESH_INTERVAL.getMillis());
                } catch (InterruptedException e1) {}
            }
        }

        private void storeLastUpdatedTimestamp(String exportTimestamp) {
            String json = "{ \"lastUpdatedTimestamp\" : \"" + exportTimestamp + "\" }";
            IndexRequest updateTimestampRequest = indexRequest(riverIndexName).type(riverName.name()).id("lastUpdatedTimestamp").source(json);
            client.index(updateTimestampRequest).actionGet();
        }

        private String getLastUpdatedTimestamp() {
            GetResponse lastUpdatedTimestampResponse = client.prepareGet().setIndex(riverIndexName).setType(riverName.name()).setId("lastUpdatedTimestamp").execute().actionGet();
            if (lastUpdatedTimestampResponse.exists() && lastUpdatedTimestampResponse.getSource().containsKey("lastUpdatedTimestamp")) {
                return lastUpdatedTimestampResponse.getSource().get("lastUpdatedTimestamp").toString();
            }

            return null;
        }
    }


    private class Indexer implements Runnable {
        private final ESLogger logger = ESLoggerFactory.getLogger(this.getClass().getName());
        private int deletedDocuments = 0;
        private int insertedDocuments = 0;
        private BulkRequestBuilder bulk;
        private StopWatch sw;

        @Override
        public void run() {
            while (!closed) {
                logger.debug("Indexer run() started");
                sw = new StopWatch().start();
                deletedDocuments = 0;
                insertedDocuments = 0;

                try {
                    RiverProduct product = stream.take();
                    bulk = client.prepareBulk();
                    do {
                        addProductToBulkRequest(product);
                    } while ((product = stream.poll(250, TimeUnit.MILLISECONDS)) != null && deletedDocuments + insertedDocuments < RIVER_MAX_BULK_SIZE);
                } catch (InterruptedException e) {
                    continue;
                } finally {
                    bulk.execute().actionGet();
                }
                logStatistics();
            }
        }

        private void addProductToBulkRequest(RiverProduct riverProduct) {
            if (riverProduct.action == RiverProduct.Action.DELETE) {
                bulk.add(deleteRequest(RIVER_INDEX).type(RIVER_TYPE).id(riverProduct.id));
                deletedDocuments++;
            } else {
                bulk.add(indexRequest(RIVER_INDEX).type(RIVER_TYPE).id(riverProduct.id).source(riverProduct.product));
                insertedDocuments++;
            }
        }

        private void logStatistics() {
            long totalDocuments = deletedDocuments + insertedDocuments;
            long totalTimeInSeconds = sw.stop().totalTime().seconds();
            long totalDocumentsPerSecond = (totalTimeInSeconds == 0) ? totalDocuments : totalDocuments / totalTimeInSeconds;
            logger.info("Indexed {} documents, {} insertions/updates, {} deletions, {} documents per second", totalDocuments, insertedDocuments, deletedDocuments, totalDocumentsPerSecond);
        }
    }
}