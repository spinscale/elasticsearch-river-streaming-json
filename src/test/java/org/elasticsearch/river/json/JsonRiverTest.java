package org.elasticsearch.river.json;

import static org.elasticsearch.common.xcontent.XContentFactory.*;

import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

import java.io.IOException;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.indices.flush.FlushRequestBuilder;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.ImmutableSettings.Builder;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import spark.Request;
import spark.Response;
import spark.Route;
import spark.Spark;

public class JsonRiverTest {

    private Node node;
    private Client client;

    @Before
    public void startNode() {
        JsonRiver.RIVER_REFRESH_INTERVAL = TimeValue.timeValueMillis(500);
        JsonRiver.RIVER_URL = "http://localhost:4567/data";

        String randStr = "UnitTestCluster" + Math.random();
        Builder settingsBuilder = ImmutableSettings.settingsBuilder();
        settingsBuilder.put("cluster.name", randStr);

        node = NodeBuilder.nodeBuilder().settings(settingsBuilder).node();
        client = node.client();

        client.prepareIndex("_river", "json", "_meta").setSource("{ \"type\": \"json\" }").execute().actionGet();
        client.admin().indices().prepareCreate("products").execute().actionGet();

        client.admin().cluster().health(new ClusterHealthRequest("products").waitForYellowStatus()).actionGet();
        client.admin().cluster().health(new ClusterHealthRequest("_river").waitForYellowStatus()).actionGet();

        IndexRequestBuilder indexRequestBuilder = new IndexRequestBuilder(client, "products");
        indexRequestBuilder.setType("product").setId("TODELETE");
        indexRequestBuilder.setSource("{ \"some\" : \"cool content\" }");
        indexRequestBuilder.execute().actionGet();

        FlushRequestBuilder builder = new FlushRequestBuilder(client.admin().indices());
        builder.setIndices("products").execute().actionGet();


/*
 * {
 *  "timestamp" : "1234",
 *  "products" : [
 *      {
 *          "action" : "index",
 *          "id":    : "1234",
 *          "product": { "name" : "sth", "spam" : "eggs" }
 *      },
 *      {
 *          "action" : "index",
 *          "id":    : "12354",
 *          "product": { "name" : "else", "basket" : "ball" }
 *      },
 *      {
 *          "action" : "delete",
 *          "id":    : "1234"
 *      }
 *  ]
 * }
 */


        Spark.get(new Route("/data") {
            @Override
            public Object handle(Request request, Response response) {
                String lastUpdate = request.queryParams("lastUpdate");
                String result = null;

                try {
                    if (lastUpdate != null) {
                        result = jsonBuilder().startObject()
                                .field("timestamp", lastUpdate+"1")
                                .endObject().string();

                    } else {
                        result = jsonBuilder().startObject()
                                    .field("timestamp", String.valueOf(System.currentTimeMillis()))
                                    .startArray("products")
                                        .startObject()
                                            .field("action", "index")
                                            .field("id", "1234")
                                            .startObject("product")
                                                .field("name", "tennisball")
                                                .field("spam", "eggs")
                                            .endObject()
                                        .endObject()
                                        .startObject()
                                            .field("action", "index")
                                            .field("id", "12345")
                                            .startObject("product")
                                                .field("name", "basketball")
                                                .field("spam", "eggs")
                                            .endObject()
                                        .endObject()
                                        .startObject()
                                            .field("action", "delete")
                                            .field("id", "TODELETE")
                                        .endObject()
                                    .endArray()
                                    .endObject().string();
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }


                return result;
            }
        });
    }

    @After
    public void closeElasticsearch() {
        client.close();
        node.close();
    }

    @Test
    public void testThatDeletionWorks() throws Exception {
        // Wait two seconds for first indexation from river
        Thread.sleep(2000);

        GetRequestBuilder builder = new GetRequestBuilder(client);
        GetResponse response = builder.setIndex("products").setType("product").setId("TODELETE").execute().actionGet();

        assertThat(response.exists(), is(false));
    }

    @Test
    public void testThatRiverWorks() throws Exception {
        Thread.sleep(2000);

        GetRequestBuilder builder = new GetRequestBuilder(client);
        GetResponse response = builder.setIndex("products").setType("product").setId("1234").execute().actionGet();

        assertThat(response.exists(), is(true));
    }

    @Test
    public void testThatLastUpdatedTimestampIsWritten() throws Exception {
        Thread.sleep(2000);

        GetRequestBuilder builder = new GetRequestBuilder(client);
        GetResponse response = builder.setIndex("_river").setType("json").setId("lastUpdatedTimestamp").execute().actionGet();

        assertThat(response.exists(), is(true));
        assertThat(response.getSource().get("lastUpdatedTimestamp").toString(), is(not(nullValue())));
    }
}
