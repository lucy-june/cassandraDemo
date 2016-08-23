import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.LatencyAwarePolicy;
import com.datastax.driver.core.policies.RoundRobinPolicy;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.datastax.driver.mapping.Result;
import com.datastax.driver.mapping.annotations.*;
import com.google.common.base.Function;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

/**
 * Created by xiaoxi on 16-8-23.
 *
 please run the following scripts first:
 ##############################################################
 use system;
 truncate sstable_activity;
 insert into sstable_activity(keyspace_name, columnfamily_name, generation,rate_120m,rate_15m) values('demo','users',1,1,1);
 insert into sstable_activity(keyspace_name, columnfamily_name, generation,rate_120m,rate_15m) values('demo','users',2,1,1);
 insert into sstable_activity(keyspace_name, columnfamily_name, generation,rate_120m,rate_15m) values('demo','users',3,1,1);
 insert into sstable_activity(keyspace_name, columnfamily_name, generation,rate_120m,rate_15m) values('demo','users',4,1,1);
 insert into sstable_activity(keyspace_name, columnfamily_name, generation,rate_120m,rate_15m) values('demo','users',5,1,1);
 insert into sstable_activity(keyspace_name, columnfamily_name, generation,rate_120m,rate_15m) values('demo','users',6,1,1);
 insert into sstable_activity(keyspace_name, columnfamily_name, generation,rate_120m,rate_15m) values('demo','users',7,1,1);
 insert into sstable_activity(keyspace_name, columnfamily_name, generation,rate_120m,rate_15m) values('demo','users',8,1,1);
 insert into sstable_activity(keyspace_name, columnfamily_name, generation,rate_120m,rate_15m) values('demo','users',9,1,1);
 insert into sstable_activity(keyspace_name, columnfamily_name, generation,rate_120m,rate_15m) values('demo','users',10,1,1);
 insert into sstable_activity(keyspace_name, columnfamily_name, generation,rate_120m,rate_15m) values('demo','users',11,1,1);
 insert into sstable_activity(keyspace_name, columnfamily_name, generation,rate_120m,rate_15m) values('demo','users',12,1,1);
 insert into sstable_activity(keyspace_name, columnfamily_name, generation,rate_120m,rate_15m) values('demo','users',13,1,1);
 ##############################################################
 */
public class Main {
    public static void main(String[] args) {
        //builder cluster connection
        Cluster cluster = Cluster.builder()
                .withClusterName("Hue Cluster")
                .addContactPoint("127.0.0.1")
                .withSocketOptions(new SocketOptions()
                        .setConnectTimeoutMillis(2000)
                        .setReadTimeoutMillis(20000)
                )
                .withLoadBalancingPolicy(LatencyAwarePolicy.builder(new RoundRobinPolicy())
                        .withExclusionThreshold(2.0)
                        .withScale(100, TimeUnit.MILLISECONDS)
                        .withRetryPeriod(10, TimeUnit.SECONDS)
                        .withUpdateRate(100, TimeUnit.MILLISECONDS)
                        .withMininumMeasurements(50)
                        .build()
                )
                .withPoolingOptions(new PoolingOptions().setCoreConnectionsPerHost(HostDistance.LOCAL, 4)
                        .setMaxConnectionsPerHost(HostDistance.LOCAL, 20)
                        .setCoreConnectionsPerHost(HostDistance.REMOTE, 2)
                        .setMaxConnectionsPerHost(HostDistance.REMOTE, 10)
                        .setHeartbeatIntervalSeconds(60))
                .build();


        //print cluster metadata
        Metadata metadata = cluster.getMetadata();
        System.out.printf("Connected to cluster: %s\n",
                metadata.getClusterName());
        for (Host host : metadata.getAllHosts()) {
            System.out.printf("Datacenter: %s; Host: %s; Rack: %s\n",
                    host.getDatacenter(), host.getAddress(), host.getRack());
        }


        //build session to a keyspace
        Session _session = cluster.connect("system");


        //run cql
        ResultSet rs = _session.execute("select * from system.local");
        rs.forEach(r -> {
            System.out.println("##############" + r);
        });


        //use simpleStatement
        SimpleStatement statement = new SimpleStatement("SELECT * from system.local");
        rs = _session.execute(statement);
        rs.forEach(r -> {
            System.out.println("&&&&&&&&&&&&&&&" + r);
        });


        //use simpleStatement with params
        SimpleStatement statement2 = new SimpleStatement("SELECT * from system.local where key=?", "local");
        rs = _session.execute(statement2);
        rs.forEach(r -> {
            System.out.println("$$$$$$$$$$$$$$$" + r);
        });


        //use queryBuilder
        Statement query = QueryBuilder.select().all().from("local");
        rs = _session.execute(query);
        rs.forEach(r -> {
            System.out.println("@@@@@@@@@@@@@@@" + r);
        });


        //use paging with out state
        Statement statement6 = new SimpleStatement("select * from sstable_activity").setFetchSize(5);
        rs = _session.execute(statement6);
        for (Row r : rs) {
            System.out.println("**************" + r);

            if (rs.getAvailableWithoutFetching() == 0 && !rs.isFullyFetched()) {
                System.out.println("^^^^^^^paging^^^^^^^");
                rs.fetchMoreResults(); // this is asynchronous
            }
        }

        //use paging with state
        Statement statement8 = new SimpleStatement("select * from sstable_activity").setFetchSize(5);
        rs = _session.execute(statement8);
        int remainingInPage = rs.getAvailableWithoutFetching();
        for (Row row : rs) {
            System.out.printf("++++++++++++++++[page row - %d] row = %s%n", remainingInPage, row);
            if (--remainingInPage == 0) break;
        }
        PagingState nextPage = rs.getExecutionInfo().getPagingState();
        System.out.println("##++ " + nextPage.toString());
        //get next page
        statement8.setPagingState(PagingState.fromString(nextPage.toString()));
        rs = _session.execute(statement8);
        int remainingInPage2 = rs.getAvailableWithoutFetching();
        for (Row row : rs) {
            System.out.printf("#+++++++++++++++[page row - %d] row = %s%n", remainingInPage2, row);
            if (--remainingInPage2 == 0) break;
        }


        //async query
        ListenableFuture<Session> session = cluster.connectAsync();
        // Use transform with an AsyncFunction to chain an async operation after another:
        ListenableFuture<ResultSet> resultSet = Futures.transform(session,
                new AsyncFunction<Session, ResultSet>() {
                    public ListenableFuture<ResultSet> apply(Session session) throws Exception {
                        return session.executeAsync("select release_version from system.local");
                    }
                });
        // Use transform with a simple Function to apply a synchronous computation on the result:
        ListenableFuture<String> version = Futures.transform(resultSet,
                new Function<ResultSet, String>() {
                    public String apply(ResultSet rs) {
                        return rs.one().getString("release_version");
                    }
                });
        // Use a callback to perform an action once the future is complete:
        Futures.addCallback(version, new FutureCallback<String>() {
            public void onSuccess(String version) {
                System.out.printf("$$$$$$$$$$$$$$$$$$$$$$   Cassandra version: %s%n", version);
            }

            public void onFailure(Throwable t) {
                System.out.printf("$$$$$$$$$$$$$$$$$$$$$$$  Failed to retrieve the version: %s%n",
                        t.getMessage());
            }
        });


        //async paging
        Session __session = cluster.connect("system");
        Statement statement3 = new SimpleStatement("select * from sstable_activity").setFetchSize(6);
        ListenableFuture<ResultSet> future = Futures.transform(__session.executeAsync(statement3), iterate(3));
        //callback
        Futures.addCallback(future, new FutureCallback<ResultSet>() {
            public void onSuccess(ResultSet rs) {
                int remainingInPage = rs.getAvailableWithoutFetching();
                System.out.println("#==============Start Page");
                for (Row row : rs) {
                    System.out.printf("#==============[page row - %d] row = %s%n", remainingInPage, row);
                    if (--remainingInPage == 0)
                        break;
                }
                System.out.println("#==============Done page");
            }

            public void onFailure(Throwable t) {
                System.out.printf("#=================Failed to retrieve",
                        t.getMessage());
            }
        });


        //object mapping
        Session session0 = cluster.connect("system");
        MappingManager manager = new MappingManager(session0);
        Mapper<SstableActivity> mapper = manager.mapper(SstableActivity.class);
        mapper.setDefaultGetOptions(Mapper.Option.tracing(true), Mapper.Option.consistencyLevel(ConsistencyLevel.QUORUM));
        mapper.setDefaultSaveOptions(Mapper.Option.ttl(60), Mapper.Option.saveNullFields(false), Mapper.Option.consistencyLevel(ConsistencyLevel.TWO));
        mapper.setDefaultDeleteOptions(Mapper.Option.consistencyLevel(ConsistencyLevel.TWO));
        SstableActivity sa = mapper.get("demo", "users", 1);
        System.out.println("%%%%%%%%%%%%" + sa.toString());

        //accessor mapping
        SstableActivityAccessor sstableActivityAccessor = manager.createAccessor(SstableActivityAccessor.class);
        Result<SstableActivity> rts = sstableActivityAccessor.getAll();
        for (SstableActivity rt : rts) {
            System.out.println("~~~~~~~~~~~~~" + rt);
        }

        Result<SstableActivity> rt = sstableActivityAccessor.getCertain("demo", "users", 1);
        System.out.println("#~~~~~~~~~~~~~" + rt.one());

        SstableActivity rt0 = sstableActivityAccessor.getCertain0("demo", "users", 1);
        System.out.println("@~~~~~~~~~~~~~" + rt0);

        SstableActivity rt1 = sstableActivityAccessor.getPart("demo", "users", 1);
        System.out.println("@~~~~~~~~~~~~~" + rt1);
    }


    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Table(name = "sstable_activity",
            caseSensitiveTable = false)
    public static class SstableActivity {
        @PartitionKey(0)
        @Column(name = "keyspace_name")
        private String keyspaceName;

        @PartitionKey(1)
        @Column(name = "columnfamily_name")
        private String columnfamilyName;

        @PartitionKey(2)
        private Integer generation;

        @Column(name = "rate_120m")
        private Double rate120m;

        @Column(name = "rate_15m")
        private Double rate15m;
    }

    @Accessor
    public interface SstableActivityAccessor {
        @Query("SELECT * FROM sstable_activity")
        Result<SstableActivity> getAll();

        @Query("SELECT * FROM sstable_activity where keyspace_name=? and columnfamily_name=? and generation=?")
        @QueryParameters(consistency = "QUORUM")
        Result<SstableActivity> getCertain(String keyspaceName, String columnfamilyName, Integer generation);

        @Query("SELECT * FROM sstable_activity where keyspace_name=? and columnfamily_name=? and generation=?")
        @QueryParameters(consistency = "QUORUM")
        SstableActivity getCertain0(String keyspaceName, String columnfamilyName, Integer generation);

        @Query("SELECT keyspace_name,rate_15m FROM sstable_activity where keyspace_name=? and columnfamily_name=? and generation=?")
        SstableActivity getPart(String keyspaceName, String columnfamilyName, Integer generation);
    }


    private static AsyncFunction<ResultSet, ResultSet> iterate(final int page) {
        return iterate(page, 1);
    }

    private static AsyncFunction<ResultSet, ResultSet> iterate(final int page, int cur) {
        return new AsyncFunction<ResultSet, ResultSet>() {
            @Override
            public ListenableFuture<ResultSet> apply(ResultSet rs) throws Exception {

                boolean wasLastPage = rs.getExecutionInfo().getPagingState() == null;
                if (rs.getExecutionInfo().getPagingState() == null && cur < page) {
                    System.out.println("===================Not enough");
                    return Futures.immediateCancelledFuture();
                } else if (cur == page) {
                    System.out.println("===================Done iterating");
                    return Futures.immediateFuture(rs);
                } else {
                    int remainingInPage = rs.getAvailableWithoutFetching();

                    System.out.printf("=================Starting page %d (%d rows)%n", page, remainingInPage);

                    for (Row row : rs) {
                        System.out.printf("===============[page %d - %d] row = %s%n", page, remainingInPage, row);
                        if (--remainingInPage == 0)
                            break;
                    }
                    System.out.printf("===============Done page %d%n", page);

                    ListenableFuture<ResultSet> future = rs.fetchMoreResults();
                    return Futures.transform(future, iterate(page, cur + 1));
                }
            }
        };
    }
}
