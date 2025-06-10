package connector;

import com.google.protobuf.ProtocolStringList;
import io.milvus.client.MilvusServiceClient;
import io.milvus.grpc.ShowCollectionsResponse;
import io.milvus.param.ConnectParam;
import io.milvus.param.R;
import io.milvus.param.collection.ShowCollectionsParam;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import java.util.*;
import java.util.concurrent.TimeUnit;

public class MilvusSchema extends AbstractSchema {
    private final String milvusConnectURL;
    private final String milvusConnectUsername;
    private final String milvusConnectPassword;
    private final String milvusConnectDatabase;

    private LinkedHashMap<String, Table> tableMaps;

    public MilvusSchema(String milvusConnectURL, String milvusConnectUsername, String milvusConnectPassword, String milvusConnectDatabase) {
        Objects.requireNonNull(milvusConnectURL);
        this.milvusConnectURL = milvusConnectURL.trim();
        this.milvusConnectUsername = milvusConnectUsername.trim();
        this.milvusConnectPassword = milvusConnectPassword.trim();
        if (milvusConnectDatabase == null || milvusConnectDatabase.trim().isEmpty()) {
            this.milvusConnectDatabase = "default";
        } else {
            this.milvusConnectDatabase = milvusConnectDatabase;
        }
    }

    @Override
    protected Map<String, Table> getTableMap() {
        if (tableMaps != null) {
            return tableMaps;
        }
        MilvusServiceClient milvusClient = getMilvusClient();
        ShowCollectionsParam showCollectionsParam = ShowCollectionsParam.newBuilder().withDatabaseName(milvusConnectDatabase).build();
        R<ShowCollectionsResponse> showCollectionsResponseR = milvusClient.showCollections(showCollectionsParam);
        handleMilvusResponseStatus(showCollectionsResponseR);
        closeMilvusClient(milvusClient);
        ProtocolStringList collectionNamesList = showCollectionsResponseR.getData().getCollectionNamesList();
        List<String> collectionNames = new ArrayList<>();
        for (Object collectionName : collectionNamesList.toArray()) {
            collectionNames.add((String) collectionName);
            System.out.println("found milvus table: " + collectionName);
        }
        tableMaps = new LinkedHashMap<>();
        for (String collectionName : collectionNames) {
            tableMaps.put(collectionName, new MilvusTable(milvusConnectDatabase, collectionName, this));
        }
        return tableMaps;
    }

    public MilvusServiceClient getMilvusClient() {
        ConnectParam connectParam = ConnectParam.newBuilder()
                .withUri(milvusConnectURL)
                .withAuthorization(milvusConnectUsername, milvusConnectPassword)
                .withDatabaseName(milvusConnectDatabase)
                .withConnectTimeout(30, TimeUnit.SECONDS)
                .build();
        MilvusServiceClient milvusServiceClient = new MilvusServiceClient(connectParam);
        return milvusServiceClient;
    }

    public void closeMilvusClient(MilvusServiceClient milvusServiceClient) {
        milvusServiceClient.close();
    }

    public void handleMilvusResponseStatus(R<?> r) {
        if (r.getStatus() != R.Status.Success.getCode()) {
            throw new RuntimeException(r.getMessage());
        }
    }
}
