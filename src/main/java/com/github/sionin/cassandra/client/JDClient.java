package com.github.sionin.cassandra.client;

import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.ExponentialReconnectionPolicy;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.github.sionin.cassandra.data.TOColumn;
import com.github.sionin.cassandra.data.TORow;
import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class JDClient implements IClient {

    public static final String CREATE_KEYSPACE_SIMPLE_FORMAT = "CREATE KEYSPACE \"%s\" WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : %d }";
    public static final String CREATE_TABLE =
            "CREATE TABLE \"%s\".\"%s\" (" +
                    "key text, " +
                    "column1 text, " +
                    "value text, " +
                    "PRIMARY KEY (key, column1)" +
                    ") WITH COMPACT STORAGE  AND caching = 'none';";
    public static final String TRUNCATE = "TRUNCATE \"%s\".\"%s\";";
    public static final String INSERT = "INSERT INTO \"%s\".\"%s\" (key, column1, value) VALUES (?, ?, ?)";


    Cluster cluster;
    Session session;
    String keyspace;
    String table;
    int fetchSize;

    PreparedStatement insert;

    public JDClient(String[] hosts, String clusterName, String keyspace, String table, int fetchSize) {
        this.keyspace = keyspace;
        this.table = table;
        this.fetchSize = fetchSize;

        Cluster.Builder builder = Cluster.builder()
                .withClusterName(clusterName)
                .addContactPoints(hosts)
                .withRetryPolicy(DefaultRetryPolicy.INSTANCE)
                .withReconnectionPolicy(new ExponentialReconnectionPolicy(10, 1000));
        cluster = builder.build();
        session = cluster.newSession();
        session.init();

        KeyspaceMetadata keyspaceMetadata = cluster.getMetadata().getKeyspace(keyspace);
        if (keyspaceMetadata == null) {
            session.execute(String.format(CREATE_KEYSPACE_SIMPLE_FORMAT, keyspace, 1));
            session.execute(String.format(CREATE_TABLE, keyspace, table));
        } else {
            TableMetadata tableMetadata = keyspaceMetadata.getTable(table);
            if (tableMetadata == null) {
                session.execute(String.format(CREATE_TABLE, keyspace, table));
            }
        }

        insert = session.prepare(String.format(INSERT, keyspace, table));
    }

    public void shutdown() {
        session.close();
        cluster.close();
    }

    public void cleanAndInsert(Iterable<TORow> rows) {
        KeyspaceMetadata keyspaceMetadata = cluster.getMetadata().getKeyspace(keyspace);
        TableMetadata tableMetadata = keyspaceMetadata.getTable(table);
        if (tableMetadata != null) {
            session.execute(String.format(TRUNCATE, keyspace, table));
        }


        Iterator<List<Statement>> inserts = Iterators.transform(rows.iterator(), new Function<TORow, List<Statement>>() {
            public List<Statement> apply(TORow row) {
                List<Statement> result = new ArrayList<Statement>();
                for (TOColumn column : row.columns) {
                    result.add(insert.bind(row.key, column.name, column.value));
                }
                return result;
            }
        });

        BatchStatement batchStatement = new BatchStatement();

        while (inserts.hasNext()) {
            List<Statement> statements = inserts.next();
            batchStatement.addAll(statements);
        }

        session.execute(batchStatement);
    }

    public List<TORow> readAll() {

        List<Row> rows = getRows();
        assert rows.size() >= fetchSize / 10;

        List<TORow> result = transformRows(rows.iterator());

        return Collections.emptyList();
    }


    public List<Row> getRows() {
        Select select = all();
        ResultSet resultSet = session.execute(select);
        return resultSet.all();
    }

    public Select all() {
        Select select = QueryBuilder.select()
                .column("key")
                .column("column1")
                .column("value")
                .writeTime("value").as("timestamp")
                .from(keyspace, table);
        select.setFetchSize(fetchSize);
        select.setConsistencyLevel(ConsistencyLevel.QUORUM);
        return select;
    }

    public List<TORow> read(List<String> keys, int partition) throws ExecutionException, InterruptedException {

        List<List<String>> partitions = Lists.partition(keys, partition);
        List<ListenableFuture<ResultSet>> futures = new ArrayList(partitions.size());

        for (List<String> keysPartition : partitions) {
            ResultSetFuture resultSetFuture = readAsync(keysPartition);
            futures.add(resultSetFuture);
        }

        Iterator<Iterator<Row>> transform = Iterators.transform(futures.iterator(), new Function<ListenableFuture<ResultSet>, Iterator<Row>>() {
            public Iterator<Row> apply(ListenableFuture<ResultSet> resultSetListenableFuture) {
                try {
                    return resultSetListenableFuture.get().iterator();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                }
                return Iterators.emptyIterator();
            }
        });


        List<TORow> result = transformRows(Iterators.concat(transform));

        return Collections.emptyList();
    }

    public ResultSetFuture readAsync(List<String> keysPartition) {
        Select.Where select = all().where(QueryBuilder.in("key", keysPartition));
        return session.executeAsync(select);
    }

    public List<TORow> transformRows(Iterator<Row> rows) {
        List<TORow> result = new ArrayList<TORow>();

        List<Row> acc = new ArrayList<Row>(128);
        ByteBuffer accKey = null;

        while (rows.hasNext()) {
            Row row = rows.next();
            ByteBuffer key = row.getBytesUnsafe(0);
            if (key.equals(accKey)) {
                acc.add(row);
            } else {
                if (accKey != null) {
                    result.add(convert(accKey, acc));
                }
                acc = new ArrayList<Row>(acc.size());
                accKey = key;
            }
        }
        return result;
    }

    private TORow convert(ByteBuffer key, List<Row> rows) {
        TORow toRow = new TORow(new String(key.array(), StandardCharsets.UTF_8));
        for (Row row : rows) {
            toRow.add(
                    row.getString(1),
                    row.getString(2),
                    row.getLong(3)
            );
        }
        return toRow;
    }
}
