package com.github.sionin.cassandra;

import com.github.sionin.cassandra.client.HectorClient;
import com.github.sionin.cassandra.client.IClient;
import com.github.sionin.cassandra.client.JDClient;
import com.github.sionin.cassandra.data.TORow;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import joptsimple.OptionParser;
import joptsimple.OptionSet;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class CassandraTest {


    public static String[] HOSTS;
    public static String CLUSTER;
    public static String KEYSPACE;
    public static String TABLE;
    public static int NUMBER_OF_ROWS;
    public static int NUMBER_OF_COLUMNS;

    public static int TEST_ITERATIONS = 100;

    private static OptionParser defaultParser() {
        OptionParser parser = new OptionParser() {{
            accepts("h", "Show this help message");
            accepts("hosts", "C* hosts").withOptionalArg().ofType(String.class).defaultsTo("localhost");
            accepts("cluster", "C* cluster").withOptionalArg().ofType(String.class).defaultsTo("testcluster");
            accepts("keyspace", "Keyspace for test").withOptionalArg().ofType(String.class).defaultsTo("testkeyspace");
            accepts("table", "Table for test").withOptionalArg().ofType(String.class).defaultsTo("testtable");
            accepts("r", "Number of row to create").withOptionalArg().ofType(Integer.class).defaultsTo(100);
            accepts("c", "Number of columns to create in every row").withOptionalArg().ofType(Integer.class).defaultsTo(500);
            accepts("n", "Number of test iterations").withOptionalArg().ofType(Integer.class).defaultsTo(100);
        }};
        return parser;
    }


    public static void main(String[] args) {

        OptionSet optionSet = defaultParser().parse(args);

        HOSTS = ((String) optionSet.valueOf("hosts")).split("[ ]*,[ ]*");
        CLUSTER = (String) optionSet.valueOf("cluster");
        KEYSPACE = (String) optionSet.valueOf("keyspace");
        TABLE = (String) optionSet.valueOf("table");
        NUMBER_OF_ROWS = (Integer) optionSet.valueOf("r");
        NUMBER_OF_COLUMNS = (Integer) optionSet.valueOf("c");
        TEST_ITERATIONS = (Integer) optionSet.valueOf("n");


        List<TORow> rows = generateTestData(NUMBER_OF_ROWS, NUMBER_OF_COLUMNS);
        List<String> keys = Lists.transform(rows, new Function<TORow, String>() {
            public String apply(TORow toRow) {
                return toRow.key;
            }
        });

        long time = System.currentTimeMillis();

        IClient client = getJDClient(1000);
        client.cleanAndInsert(rows);
        client.shutdown();

        time = System.currentTimeMillis() - time;
        System.out.println("Data prepare time: " + time);
        System.out.println("\n");

        System.out.println("Test java-driver partitions");
        IClient jdClient = getJDClient(15000);
        for (int partition = 5; partition < 101; partition += 5) {
            testReadKeys(jdClient, rows, keys, partition);
        }
        for (int partition = 5; partition < 101; partition += 5) {
            testReadKeys(jdClient, rows, keys, partition);
        }
        jdClient.shutdown();

//        System.out.println("Test java-driver fetchSize = " + (NUMBER_OF_ROWS * NUMBER_OF_COLUMNS / 2));
//        IClient jdClient2 = getJDClient(NUMBER_OF_ROWS * NUMBER_OF_COLUMNS / 2);
//        testReadAll(jdClient2, rows);
//        jdClient2.shutdown();
//
//        System.out.println("Test java-driver fetchSize = " + (NUMBER_OF_ROWS * NUMBER_OF_COLUMNS));
//        IClient jdClient3 = getJDClient((NUMBER_OF_ROWS * NUMBER_OF_COLUMNS));
//        testReadAll(jdClient3, rows);
//        jdClient3.shutdown();
//
//
        System.out.println("Tests hector fetchSize = " + NUMBER_OF_ROWS / 2);
        IClient hectorClient = getHectorClient(NUMBER_OF_ROWS / 2);
        testReadAll(hectorClient, rows);
        hectorClient.shutdown();

        System.out.println("Tests hector fetchSize = " + NUMBER_OF_ROWS);
        IClient hectorClient2 = getHectorClient(NUMBER_OF_ROWS);
        testReadAll(hectorClient2, rows);
        hectorClient2.shutdown();

        System.exit(0);
    }

    private static List<TORow> generateTestData(int rowsN, int columnsN) {
        List<TORow> rows = new ArrayList<TORow>();
        for (int i = 0; i < rowsN; i++) {
            TORow row = new TORow("Row" + i);
            for (int j = 0; j < columnsN; j++) {
                row.add("column" + j, UUID.randomUUID().toString());
            }
            rows.add(row);
        }
        return rows;
    }

    public static IClient getJDClient(int fetchSize) {
        return new JDClient(HOSTS, CLUSTER, KEYSPACE, TABLE, fetchSize);
    }

    public static IClient getHectorClient(int fetchSize) {
        return new HectorClient(HOSTS, CLUSTER, KEYSPACE, TABLE, fetchSize);
    }

    public static void testReadKeys(IClient client, List<TORow> expected, List<String> keys, int partition) {
        long testTime = System.currentTimeMillis();

//        for (int i = 0; i < TEST_ITERATIONS / 10; i++) {
//            List<TORow> allRows = client.readAll();
//            assertRows(expected, allRows);
//        }

        long cleanTime = 0L;
        for (int i = 0; i < TEST_ITERATIONS; i++) {
            long iterTime = System.currentTimeMillis();

            List<TORow> allRows = client.read(keys, partition);

            iterTime = System.currentTimeMillis() - iterTime;
//            System.out.println("   Test loop iteration time: " + iterTime);
            cleanTime += iterTime;

            assertRows(expected, allRows);
        }

        System.out.println(" Partition: " + partition + " average time: " + cleanTime / TEST_ITERATIONS);

        testTime = System.currentTimeMillis() - testTime;
//        System.out.println(" Full test time: " + testTime);
//        System.out.println(" Clean read time: " + cleanTime);
//        System.out.println(" Average iteration time: " + cleanTime / TEST_ITERATIONS);
//        System.out.println("\n");
    }

    public static void testReadAll(IClient client, List<TORow> expected) {
        long testTime = System.currentTimeMillis();

        for (int i = 0; i < TEST_ITERATIONS / 10; i++) {
            List<TORow> allRows = client.readAll();
            assertRows(expected, allRows);
        }

        long cleanTime = 0L;
        for (int i = 0; i < TEST_ITERATIONS; i++) {
            long iterTime = System.currentTimeMillis();

            List<TORow> allRows = client.readAll();

            iterTime = System.currentTimeMillis() - iterTime;
//            System.out.println("   Test loop iteration time: " + iterTime);
            cleanTime += iterTime;

            assertRows(expected, allRows);
        }


        testTime = System.currentTimeMillis() - testTime;
        System.out.println(" Full test time: " + testTime);
        System.out.println(" Clean read time: " + cleanTime);
        System.out.println(" Average iteration time: " + cleanTime / TEST_ITERATIONS);
        System.out.println("\n");
    }

    private static void assertRows(List<TORow> expected, List<TORow> allRows) {
//        assert expected.size() == allRows.size();
    }
}
