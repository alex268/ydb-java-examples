package tech.ydb.example;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tech.ydb.auth.iam.CloudAuthHelper;
import tech.ydb.common.transaction.TxMode;
import tech.ydb.core.Status;
import tech.ydb.core.grpc.GrpcReadStream;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.query.QueryClient;
import tech.ydb.table.SessionRetryContext;
import tech.ydb.table.TableClient;
import tech.ydb.table.description.TableColumn;
import tech.ydb.table.description.TableDescription;
import tech.ydb.table.query.DataQueryResult;
import tech.ydb.table.query.Params;
import tech.ydb.table.result.ResultSetReader;
import tech.ydb.table.settings.BulkUpsertSettings;
import tech.ydb.table.settings.ExecuteScanQuerySettings;
import tech.ydb.table.transaction.TableTransaction;
import tech.ydb.table.transaction.TxControl;
import tech.ydb.table.values.ListType;
import tech.ydb.table.values.ListValue;
import tech.ydb.table.values.PrimitiveType;
import tech.ydb.table.values.PrimitiveValue;
import tech.ydb.table.values.StructType;


public final class App implements Runnable, AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(App.class);

    private final GrpcTransport transport;
    private final TableClient tableClient;
    private final String database;
    private final SessionRetryContext retryCtx;

    App(String connectionString) {
        this.transport = GrpcTransport.forConnectionString(connectionString)
                .withAuthProvider(CloudAuthHelper.getAuthProviderFromEnviron())
                .withConnectTimeout(Duration.ofHours(1))
                .build();
        this.tableClient = QueryClient.newTableClient(transport).build();

        this.database = transport.getDatabase();
        this.retryCtx = SessionRetryContext.create(tableClient).build();
    }

    @Override
    public void run() {
        createTables();
        describeTables();
        upsertTablesData();

        upsertSimple();

        selectSimple();
        selectWithParams(1, 2);
        scanQueryWithParams(2, 1);

        multiStepTransaction(2, 5);
        tclTransaction();

        dropTables();
    }

    @Override
    public void close() {
        tableClient.close();
        transport.close();
    }

    private void createTables() {
        TableDescription seriesTable = TableDescription.newBuilder()
            .addNonnullColumn("series_id", PrimitiveType.Uint64)
            .addNullableColumn("title", PrimitiveType.Text)
            .addNullableColumn("series_info", PrimitiveType.Text)
            .addNullableColumn("release_date", PrimitiveType.Date)
            .setPrimaryKey("series_id")
            .build();

        retryCtx.supplyStatus(session -> session.createTable(database + "/series", seriesTable))
                .join().expectSuccess("Can't create table /series");

        TableDescription seasonsTable = TableDescription.newBuilder()
            .addNonnullColumn("series_id", PrimitiveType.Uint64)
            .addNonnullColumn("season_id", PrimitiveType.Uint64)
            .addNullableColumn("title", PrimitiveType.Text)
            .addNullableColumn("first_aired", PrimitiveType.Date)
            .addNullableColumn("last_aired", PrimitiveType.Date)
            .setPrimaryKeys("series_id", "season_id")
            .build();

        retryCtx.supplyStatus(session -> session.createTable(database + "/seasons", seasonsTable))
                .join().expectSuccess("Can't create table /seasons");

        TableDescription episodesTable = TableDescription.newBuilder()
            .addNonnullColumn("series_id", PrimitiveType.Uint64)
            .addNonnullColumn("season_id", PrimitiveType.Uint64)
            .addNonnullColumn("episode_id", PrimitiveType.Uint64)
            .addNullableColumn("title", PrimitiveType.Text)
            .addNullableColumn("air_date", PrimitiveType.Date)
            .setPrimaryKeys("series_id", "season_id", "episode_id")
            .build();

        retryCtx.supplyStatus(session -> session.createTable(database + "/episodes", episodesTable))
                .join().expectSuccess("Can't create table /episodes");
    }

    private void describeTables() {
        logger.info("--[ DescribeTables ]--");

        Arrays.asList("series", "seasons", "episodes").forEach(tableName -> {
            String tablePath = database + '/' + tableName;
            TableDescription tableDesc = retryCtx.supplyResult(session -> session.describeTable(tablePath))
                    .join().getValue();

            List<String> primaryKeys = tableDesc.getPrimaryKeys();
            logger.info("  table {}", tableName);
            for (TableColumn column : tableDesc.getColumns()) {
                boolean isPrimary = primaryKeys.contains(column.getName());
                logger.info("     {}: {} {}", column.getName(), column.getType(), isPrimary ? " (PK)" : "");
            }
        });
    }

    private void upsertTablesData() {
        // Create type for struct of series
        StructType seriesType = StructType.of(
                "series_id", PrimitiveType.Uint64,
                "title", PrimitiveType.Text,
                "release_date", PrimitiveType.Date,
                "series_info", PrimitiveType.Text
        );
        // Create and fill list of series
        ListValue seriesData = ListType.of(seriesType).newValue(
                SeriesData.SERIES.stream().map(series -> seriesType.newValue(
                        "series_id", PrimitiveValue.newUint64(series.seriesID()),
                        "title", PrimitiveValue.newText(series.title()),
                        "release_date", PrimitiveValue.newDate(series.releaseDate()),
                        "series_info", PrimitiveValue.newText(series.seriesInfo())
                )).collect(Collectors.toList())
        );
        // Upsert list of series to table
        retryCtx.supplyStatus(session -> session.executeBulkUpsert(
                database + "/series", seriesData, new BulkUpsertSettings()
        )).join().expectSuccess("bulk upsert problem");


        // Create type for struct of season
        StructType seasonType = StructType.of(
                "series_id", PrimitiveType.Uint64,
                "season_id", PrimitiveType.Uint64,
                "title", PrimitiveType.Text,
                "first_aired", PrimitiveType.Date,
                "last_aired", PrimitiveType.Date
        );
        // Create and fill list of seasons
        ListValue seasonsData = ListType.of(seasonType).newValue(
                SeriesData.SEASONS.stream().map(season -> seasonType.newValue(
                        "series_id", PrimitiveValue.newUint64(season.seriesID()),
                        "season_id", PrimitiveValue.newUint64(season.seasonID()),
                        "title", PrimitiveValue.newText(season.title()),
                        "first_aired", PrimitiveValue.newDate(season.firstAired()),
                        "last_aired", PrimitiveValue.newDate(season.lastAired())
                )).collect(Collectors.toList())
        );
        // Upsert list of series to seasons
        retryCtx.supplyStatus(session -> session.executeBulkUpsert(
                database + "/seasons", seasonsData, new BulkUpsertSettings()
        )).join().expectSuccess("bulk upsert problem");


        // Create type for struct of episode
        StructType episodeType = StructType.of(
                "series_id", PrimitiveType.Uint64,
                "season_id", PrimitiveType.Uint64,
                "episode_id", PrimitiveType.Uint64,
                "title", PrimitiveType.Text,
                "air_date", PrimitiveType.Date
        );
        // Create and fill list of episodes
        ListValue episodesData = ListType.of(episodeType).newValue(
                SeriesData.EPISODES.stream().map(episode -> episodeType.newValue(
                        "series_id", PrimitiveValue.newUint64(episode.seriesID()),
                        "season_id", PrimitiveValue.newUint64(episode.seasonID()),
                        "episode_id", PrimitiveValue.newUint64(episode.episodeID()),
                        "title", PrimitiveValue.newText(episode.title()),
                        "air_date", PrimitiveValue.newDate(episode.airDate())
                )).collect(Collectors.toList())
        );

        // Upsert list of series to episodes
        retryCtx.supplyStatus(session -> session.executeBulkUpsert(
                database + "/episodes", episodesData, new BulkUpsertSettings()
        )).join().expectSuccess("bulk upsert problem");
    }

    private void upsertSimple() {
        String query
                = "UPSERT INTO episodes (series_id, season_id, episode_id, title) "
                + "VALUES (2, 6, 1, \"TBD\");";

        // Begin new transaction with SerializableRW mode
        TxControl<?> txControl = TxControl.serializableRw().setCommitTx(true);

        // Executes data query with specified transaction control settings.
        retryCtx.supplyResult(session -> session.executeDataQuery(query, txControl))
            .join().getValue();
    }

    private void selectSimple() {
        String query
                = "SELECT series_id, title, release_date "
                + "FROM series WHERE series_id = 1;";

        // Begin new transaction with SerializableRW mode
        TxControl<?> txControl = TxControl.serializableRw().setCommitTx(true);

        // Executes data query with specified transaction control settings.
        DataQueryResult result = retryCtx.supplyResult(session -> session.executeDataQuery(query, txControl))
                .join().getValue();

        logger.info("--[ SelectSimple ]--");

        ResultSetReader rs = result.getResultSet(0);
        while (rs.next()) {
            logger.info("read series with id {}, title {} and release_date {}",
                    rs.getColumn("series_id").getUint64(),
                    rs.getColumn("title").getText(),
                    rs.getColumn("release_date").getDate()
            );
        }
    }

    private void selectWithParams(long seriesID, long seasonID) {
        String query
                = "DECLARE $seriesId AS Uint64; "
                + "DECLARE $seasonId AS Uint64; "
                + "SELECT sa.title AS season_title, sr.title AS series_title "
                + "FROM seasons AS sa INNER JOIN series AS sr ON sa.series_id = sr.series_id "
                + "WHERE sa.series_id = $seriesId AND sa.season_id = $seasonId";

        // Begin new transaction with SerializableRW mode
        TxControl<?> txControl = TxControl.serializableRw().setCommitTx(true);

        // Type of parameter values should be exactly the same as in DECLARE statements.
        Params params = Params.of(
                "$seriesId", PrimitiveValue.newUint64(seriesID),
                "$seasonId", PrimitiveValue.newUint64(seasonID)
        );

        DataQueryResult result = retryCtx.supplyResult(session -> session.executeDataQuery(query, txControl, params))
                .join().getValue();

        logger.info("--[ SelectWithParams ] -- ");

        ResultSetReader rs = result.getResultSet(0);
        while (rs.next()) {
            logger.info("read season with title {} for series {}",
                    rs.getColumn("season_title").getText(),
                    rs.getColumn("series_title").getText()
            );
        }
    }

    private void scanQueryWithParams(long seriesID, long seasonID) {
        String query
                = "DECLARE $seriesId AS Uint64; "
                + "DECLARE $seasonId AS Uint64; "
                + "SELECT ep.title AS episode_title, sa.title AS season_title, sr.title AS series_title "
                + "FROM episodes AS ep "
                + "JOIN seasons AS sa ON sa.season_id = ep.season_id "
                + "JOIN series AS sr ON sr.series_id = sa.series_id "
                + "WHERE sa.series_id = $seriesId AND sa.season_id = $seasonId;";

        // Type of parameter values should be exactly the same as in DECLARE statements.
        Params params = Params.of(
                "$seriesId", PrimitiveValue.newUint64(seriesID),
                "$seasonId", PrimitiveValue.newUint64(seasonID)
        );

        logger.info("--[ ExecuteScanQueryWithParams ]--");
        retryCtx.supplyStatus(session -> {
            ExecuteScanQuerySettings settings = ExecuteScanQuerySettings.newBuilder().build();
            GrpcReadStream<ResultSetReader> scan = session.executeScanQuery(query, params, settings);
            return scan.start(rs -> {
                while (rs.next()) {
                    logger.info("read episode {} of {} for {}",
                            rs.getColumn("episode_title").getText(),
                            rs.getColumn("season_title").getText(),
                            rs.getColumn("series_title").getText()
                    );
                }
            });
        }).join().expectSuccess("scan query problem");
    }

    private void multiStepTransaction(long seriesID, long seasonID) {
        retryCtx.supplyStatus(session -> {
            TableTransaction transaction = session.createNewTransaction(TxMode.SERIALIZABLE_RW);
            String query1
                    = "DECLARE $seriesId AS Uint64; "
                    + "DECLARE $seasonId AS Uint64; "
                    + "SELECT MIN(first_aired) AS from_date FROM seasons "
                    + "WHERE series_id = $seriesId AND season_id = $seasonId;";

            // Execute first query to get the required values to the client.
            // Transaction control settings don't set CommitTx flag to keep transaction active
            // after query execution.
            DataQueryResult res1 = transaction.executeDataQuery(query1, Params.of(
                    "$seriesId", PrimitiveValue.newUint64(seriesID),
                    "$seasonId", PrimitiveValue.newUint64(seasonID)
            )).join().getValue();

            // Perform some client logic on returned values
            ResultSetReader resultSet = res1.getResultSet(0);
            if (!resultSet.next()) {
                throw new RuntimeException("not found first_aired");
            }
            LocalDate fromDate = resultSet.getColumn("from_date").getDate();
            LocalDate toDate = fromDate.plusDays(15);

            // Get active transaction id
            logger.info("got transaction id {}", transaction.getId());

            // Construct next query based on the results of client logic
            String query2
                    = "DECLARE $seriesId AS Uint64;"
                    + "DECLARE $fromDate AS Date;"
                    + "DECLARE $toDate AS Date;"
                    + "SELECT season_id, episode_id, title, air_date FROM episodes "
                    + "WHERE series_id = $seriesId AND air_date >= $fromDate AND air_date <= $toDate;";

            // Execute second query.
            // Transaction control settings continues active transaction (tx) and
            // commits it at the end of second query execution.
            DataQueryResult res2 = transaction.executeDataQueryAndCommit(query2, Params.of(
                "$seriesId", PrimitiveValue.newUint64(seriesID),
                "$fromDate", PrimitiveValue.newDate(fromDate),
                "$toDate", PrimitiveValue.newDate(toDate)
            )).join().getValue();

            logger.info("--[ MultiStep ]--");
            ResultSetReader rs = res2.getResultSet(0);
            while (rs.next()) {
                logger.info("read episode {} with air date {}",
                        rs.getColumn("title").getText(),
                        rs.getColumn("air_date").getDate()
                );
            }

            return CompletableFuture.completedFuture(Status.SUCCESS);
        }).join().expectSuccess("multistep transaction problem");
    }

    private void tclTransaction() {
        retryCtx.supplyStatus(session -> {
            // Create new transaction.
            // It is not active and has no id until any query is executed on it
            TableTransaction transaction = session.createNewTransaction(TxMode.SERIALIZABLE_RW);

            String query
                    = "DECLARE $airDate AS Date; "
                    + "UPDATE episodes SET air_date = $airDate WHERE title = \"TBD\";";

            Params params = Params.of("$airDate", PrimitiveValue.newDate(Instant.now()));

            // Execute data query on new transaction.
            // Transaction will be created on server and become active on client
            // Query will be executed on it, but transaction will not be committed
            DataQueryResult result = transaction.executeDataQuery(query, params)
                .join().getValue();

            logger.info("get transaction {}", result.getTxId());

            // Commit active transaction (tx)
            return transaction.commit();
        }).join().expectSuccess("tcl transaction problem");
    }

    private void dropTables() {
        retryCtx.supplyStatus(session -> session.dropTable(database + "/episodes"))
                .join().expectSuccess("drop table /episodes problem");
        retryCtx.supplyStatus(session -> session.dropTable(database + "/seasons"))
                .join().expectSuccess("drop table /seasons problem");
        retryCtx.supplyStatus(session -> session.dropTable(database + "/series"))
                .join().expectSuccess("drop table /series problem");
    }

    public static void main(String[] args) throws IOException {
        if (args.length != 1) {
            System.err.println("Usage: java -jar ydb-basic-example.jar <connection-string>");
            return;
        }

        try (App app = new App(args[0])) {
            app.run();
        } catch (Exception e) {
            logger.error("app problem", e);
        }
    }
}
