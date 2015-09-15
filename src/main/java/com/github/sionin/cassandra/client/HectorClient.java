package com.github.sionin.cassandra.client;

import com.github.sionin.cassandra.data.TORow;
import com.google.common.base.Joiner;
import me.prettyprint.cassandra.model.CqlQuery;
import me.prettyprint.cassandra.model.CqlRows;
import me.prettyprint.cassandra.serializers.BytesArraySerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.QueryResult;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class HectorClient implements IClient {

    public static final String SELECT = "SELECT * from %s";
    Cluster cluster;
    Keyspace keyspace;
    String keyspaceName;
    String table;
    int fetchSize;

    public HectorClient(String[] hosts, String clusterName, String keyspaceName, String table, int fetchSize) {
        this.keyspaceName = keyspaceName;
        this.table = table;
        this.fetchSize = fetchSize;

        CassandraHostConfigurator hostConfigurator = new CassandraHostConfigurator();
        hostConfigurator.setAutoDiscoverHosts(false);
        hostConfigurator.setRetryDownedHosts(false);
        hostConfigurator.setUseHostTimeoutTracker(false);
        hostConfigurator.setHosts(Joiner.on(",").join(hosts));

        cluster = HFactory.getOrCreateCluster(clusterName, hostConfigurator);

        KeyspaceDefinition keyspaceDefinition = cluster.describeKeyspace(keyspaceName);
        if (keyspaceDefinition == null) {
            shutdown();
            throw new UnsupportedOperationException("Keyspace " + keyspaceName + " does not exist");
        } else {
            keyspace = HFactory.createKeyspace(keyspaceName, cluster);
        }

    }

    public void shutdown() {
        HFactory.shutdownCluster(cluster);
    }

    public void cleanAndInsert(Iterable<TORow> rows) {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    public List<TORow> readAll() {

        List<Row<String, String, byte[]>> rows = getRows();
        assert rows.size() >= fetchSize / 10;

        List<TORow> result = transformRows(rows);

        return Collections.emptyList();
    }

    public List<TORow> read(List<String> keys, int partition) {
        return readAll();
    }

    public List<Row<String, String, byte[]>> getRows() {
        String select = String.format(SELECT, table);
        CqlQuery<String, String, byte[]> cqlQuery = new CqlQuery(keyspace, StringSerializer.get(), StringSerializer.get(), BytesArraySerializer.get());
        cqlQuery.setQuery(select);
        cqlQuery.setSuppressKeyInColumns(true);
        QueryResult<CqlRows<String, String, byte[]>> queryResult = cqlQuery.execute();

        return queryResult.get().getList();
    }

    public List<TORow> transformRows(List<Row<String, String, byte[]>> rows) {
        List<TORow> result = new ArrayList<TORow>();

        for (Row<String, String, byte[]> row : rows) {
            List<HColumn<String, byte[]>> columns = row.getColumnSlice().getColumns();
            if (!columns.isEmpty()) {
                TORow toRow = new TORow(row.getKey());
                for (HColumn<String, byte[]> column : columns) {
                    toRow.add(
                            column.getName(),
                            new String(column.getValue(), StandardCharsets.UTF_8),
                            column.getClock()
                    );
                }
            }
        }
        return result;
    }


}
