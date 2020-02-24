package io.pravega.schemaregistry.storage.client;

import io.pravega.client.tables.impl.KeyVersion;
import io.pravega.client.tables.impl.TableEntry;
import io.pravega.client.tables.impl.TableKey;
import io.pravega.client.tables.impl.TableKeyImpl;
import io.pravega.common.Exceptions;
import io.pravega.common.tracing.RequestTag;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.server.WireCommandFailedException;
import io.pravega.controller.server.rpc.auth.GrpcAuthHelper;
import io.pravega.controller.store.stream.PravegaTablesStoreHelper;
import io.pravega.controller.store.stream.StoreException;
import io.pravega.controller.store.stream.Version;
import io.pravega.controller.store.stream.VersionedMetadata;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.shaded.com.google.common.base.Charsets;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
public class TableStore extends PravegaTablesStoreHelper {
    private static final String NON_EXISTENT_KEY = "NonExistentKey";
    
    private final SegmentHelper segmentHelper;
    private final GrpcAuthHelper authHelper;
    private final AtomicReference<String> authToken = new AtomicReference<>();
    
    public TableStore(SegmentHelper segmentHelper, GrpcAuthHelper authHelper, ScheduledExecutorService executor) {
        super(segmentHelper, authHelper, executor);
        this.segmentHelper = segmentHelper;
        this.authHelper = authHelper;
        this.authToken.set(authHelper.retrieveMasterToken());
    }

    public CompletableFuture<Boolean> checkTableExists(String tableName) {
        List<TableKey<byte[]>> keys = Collections.singletonList(new TableKeyImpl<>(NON_EXISTENT_KEY.getBytes(Charsets.UTF_8), null));
        return segmentHelper.readTable(tableName, keys, authToken.get(), RequestTag.NON_EXISTENT_ID)
                     .thenApply(v -> true)
                     .exceptionally(e -> {
                         Throwable cause = Exceptions.unwrap(e);
                         if (cause instanceof WireCommandFailedException) {
                             WireCommandFailedException wcfe = (WireCommandFailedException) cause;
                             switch (wcfe.getReason()) {
                                 case SegmentDoesNotExist:
                                     return false;
                                 case TableKeyDoesNotExist:
                                     return true;
                                 case ConnectionDropped:
                                 case ConnectionFailed:
                                 case UnknownHost:
                                     throw new CompletionException(StoreException.create(StoreException.Type.CONNECTION_ERROR, wcfe));
                                 case AuthFailed:
                                     authToken.set(authHelper.retrieveMasterToken());
                                     throw new CompletionException(StoreException.create(StoreException.Type.CONNECTION_ERROR, wcfe));
                                 default:
                                     throw new CompletionException(StoreException.create(StoreException.Type.UNKNOWN, wcfe));
                             }
                         } else {
                             throw new CompletionException(StoreException.create(StoreException.Type.UNKNOWN, e));
                         }
                     });
    }
}
