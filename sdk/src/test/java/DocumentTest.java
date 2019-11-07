import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.microsoft.azure.cosmosdb.*;
import com.microsoft.azure.cosmosdb.rx.AsyncDocumentClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import rx.Observable;
import rx.schedulers.Schedulers;

import java.util.Map;
import java.util.UUID;

public class DocumentTest {
    private static AsyncDocumentClient client;

    private static final String DATABASE = "sdk_test";
    private static final String DATABASE_LINK = "/dbs/" + DATABASE;
    private static final String CONTAINER = "test";
    private static final String CONTAINER_LINK = DATABASE_LINK + "/colls/" + CONTAINER;

    private static final Logger logger = LoggerFactory.getLogger(DocumentTest.class);

    @BeforeClass
    public static void init() {
        ConnectionPolicy policy = new ConnectionPolicy();
        policy.setConnectionMode(ConnectionMode.Direct);
        client = new AsyncDocumentClient.Builder()
                .withServiceEndpoint(System.getenv("COSMOS_ENDPOINT"))
                .withMasterKeyOrResourceToken(System.getenv("COSMOS_KEY"))
                .withConnectionPolicy(policy)
                .withConsistencyLevel(ConsistencyLevel.Session)
                .build();
    }

    private static void createContainer() {
        client.readDatabase(DATABASE_LINK, null)
                .onErrorResumeNext(e -> {
                    if (!isNotFound(e)) {
                        return Observable.error(e);
                    }
                    logger.info("Creating database: {}", DATABASE);
                    Database db = new Database();
                    db.setId(DATABASE);
                    return client.createDatabase(db, null);
                })
                .map(ResourceResponse::getResource)
                .toBlocking().single();

        client.readCollection(CONTAINER_LINK, null)
                .onErrorResumeNext(e -> {
                    if (!isNotFound(e)) {
                        return Observable.error(e);
                    }
                    logger.info("Creating container: {}", CONTAINER);
                    DocumentCollection c = new DocumentCollection();
                    c.setId(CONTAINER);
                    PartitionKeyDefinition key = new PartitionKeyDefinition();
                    key.setPaths(ImmutableList.of("/id"));
                    c.setPartitionKey(key);
                    return client.createCollection(DATABASE_LINK, c, null);
                })
                .map(ResourceResponse::getResource)
                .toBlocking().single();
    }

    private static boolean isNotFound(Throwable e) {
        return e instanceof DocumentClientException && ((DocumentClientException) e).getStatusCode() == 404;
    }

    @AfterClass
    public static void destroy() {
        client.close();
    }

    @Test
    public void create() {
        Data created = client.createDocument(CONTAINER_LINK,
                new Data(ImmutableMap.of("name", "David Connelly")), null, true)
                .subscribeOn(Schedulers.computation())
                .doOnNext(item -> logger.info("Finished creating document"))
                .map(r -> r.getResource().toObject(Data.class))
                .toBlocking().single();
    }

    private static class Data {
        @JsonProperty
        String id;
        @JsonProperty
        Map<String, String> data;

        public Data() {}

        public Data(Map<String, String> data) {
            this.id = UUID.randomUUID().toString();
            this.data = data;
        }
    }
}
