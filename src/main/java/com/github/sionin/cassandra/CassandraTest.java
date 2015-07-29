package com.github.sionin.cassandra;

import com.github.sionin.cassandra.client.HectorClient;
import com.github.sionin.cassandra.client.IClient;
import com.github.sionin.cassandra.client.JDClient;
import com.github.sionin.cassandra.data.TORow;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class CassandraTest {


    public static final String[] HOSTS = new String[]{"localhost"};
    public static final String CLUSTER = "testcluster";
    public static final String KEYSPACE = "testkeyspace";
    public static final String TABLE = "testtable";


    public static final int TEST_ITERATIONS = 100;


    public static final int NUMBER_OF_ROWS = 100;
    public static final int NUMBER_OF_COLUMNS = 500;

    public static void main(String[] args) {

        List<TORow> rows = generateTestData(NUMBER_OF_ROWS, NUMBER_OF_COLUMNS);

        long time = System.currentTimeMillis();

        IClient client = getJDClient(1000);
        client.cleanAndInsert(rows);
        client.shutdown();

        time = System.currentTimeMillis() - time;
        System.out.println("Data prepare time: " + time);


//        System.out.println("Test java-driver fetchSize = " + NUMBER_OF_ROWS / 2);
//        IClient jdClient = getJDClient(NUMBER_OF_ROWS / 2);
//        test(jdClient, rows);
//        jdClient.shutdown();

        System.out.println("Test java-driver fetchSize = " + (NUMBER_OF_ROWS * NUMBER_OF_COLUMNS / 2));
        IClient jdClient2 = getJDClient(NUMBER_OF_ROWS * NUMBER_OF_COLUMNS / 2);
        test(jdClient2, rows);
        jdClient2.shutdown();

        System.out.println("Test java-driver fetchSize = " + (NUMBER_OF_ROWS * NUMBER_OF_COLUMNS));
        IClient jdClient3 = getJDClient((NUMBER_OF_ROWS * NUMBER_OF_COLUMNS));
        test(jdClient3, rows);
        jdClient3.shutdown();


        System.out.println("Tests hector fetchSize = " + NUMBER_OF_ROWS / 2);
        IClient hectorClient = getHectorClient(NUMBER_OF_ROWS / 2);
        test(hectorClient, rows);
        hectorClient.shutdown();

        System.out.println("Tests hector fetchSize = " + NUMBER_OF_ROWS);
        IClient hectorClient2 = getHectorClient(NUMBER_OF_ROWS);
        test(hectorClient2, rows);
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

    public static void test(IClient client, List<TORow> expected) {
        long testTime = System.currentTimeMillis();

        for (int i = 0; i < 10; i++) {
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
