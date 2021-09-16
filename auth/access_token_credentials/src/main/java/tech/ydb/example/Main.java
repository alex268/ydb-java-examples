package tech.ydb.example;

import tech.ydb.auth.iam.CloudAuthProvider;
import tech.ydb.core.Result;
import tech.ydb.core.auth.AuthProvider;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.table.Session;
import tech.ydb.table.TableClient;
import tech.ydb.table.result.ResultSetReader;
import tech.ydb.table.rpc.grpc.GrpcTableRpc;
import tech.ydb.table.transaction.TxControl;

import java.time.Duration;

import yandex.cloud.sdk.auth.provider.IamTokenCredentialProvider;

public final class Main {
    public static void main(String[] args) {
        if (args.length != 2) {
            System.err.println("Usage: java -jar example.jar <endpoint> <database> <access-token>");
        }
        String endpoint = args[0];
        String database = args[1];
        String accessToken = args[2];

        AuthProvider authProvider = CloudAuthProvider.newAuthProvider(
            IamTokenCredentialProvider.builder()
                .token(accessToken)
                .build()
        );

        GrpcTransport transport = GrpcTransport.forEndpoint(endpoint, database)
                .withAuthProvider(authProvider)
                .withSecureConnection()
                .build();
        TableClient tableClient = TableClient.newClient(GrpcTableRpc.useTransport(transport))
                .build();
        Result<Session> sessionResult = tableClient.getOrCreateSession(Duration.ofSeconds(10))
                .join();
        Session session = sessionResult.expect("ok");
        ResultSetReader rsReader = session.executeDataQuery("SELECT 1;", TxControl.serializableRw()).join()
                .expect("ok").getResultSet(0);
        assert(rsReader.getRowCount() == 1);
        System.out.println("Result:");
        while (rsReader.next()) {
            System.out.println(rsReader.getColumn(0).getInt32());
        }
    }
}