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

    public static void main(String[] args) {


        List<TORow> rows = generateTestData(100, 500);

        long time = System.currentTimeMillis();

        IClient client = getJDClient(50 * 100);
        client.cleanAndInsert(rows);
        client.shutdown();

        time = System.currentTimeMillis() - time;
        System.out.println("Data prepare time: " + time);


        System.out.println("Test java-driver fetchSize = " + 50);
        IClient jdClient = getJDClient(50);
        test(jdClient, rows);
        jdClient.shutdown();

        System.out.println("Test java-driver fetchSize = " + 5000);
        IClient jdClient2 = getJDClient(5000);
        test(jdClient2, rows);
        jdClient2.shutdown();

        System.out.println("Test java-driver fetchSize = " + 10000);
        IClient jdClient3 = getJDClient(10000);
        test(jdClient3, rows);
        jdClient3.shutdown();


        System.out.println("Tests hector fetchSize = " + 50);
        IClient hectorClient = getHectorClient(50);
        test(hectorClient, rows);
        hectorClient.shutdown();

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

        long fullTime = 0L;
        for (int i = 0; i < 100; i++) {
            long iterTime = System.currentTimeMillis();

            List<TORow> allRows = client.readAll();

            iterTime = System.currentTimeMillis() - iterTime;
//            System.out.println("   Test loop iteration time: " + iterTime);
            fullTime += iterTime;

            assertRows(expected, allRows);
        }


        testTime = System.currentTimeMillis() - testTime;
        System.out.println(" Full test time: " + testTime);
        System.out.println(" Clean read time: " + fullTime);
        System.out.println(" Average iteration time: " + fullTime / 100);
    }

    private static void assertRows(List<TORow> expected, List<TORow> allRows) {
        assert expected.size() == allRows.size();
    }
}
