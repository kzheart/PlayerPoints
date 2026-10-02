package org.black_ixx.playerpoints.storage;

import com.google.common.cache.CacheBuilder;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import org.black_ixx.playerpoints.models.PendingTransaction;

/** Display I/O cannot occupy the ordered transaction queue. All published values are committed revisions. */
public final class PgPointsWallet implements AutoCloseable {
    private final PgPointsRepository repository;
    private final ThreadPoolExecutor writer = executor("PlayerPoints-PG-write", 10000);
    private final ThreadPoolExecutor reader = executor("PlayerPoints-PG-read", 256);
    private final ConcurrentMap<UUID, PgPointsRepository.Balance> cache = CacheBuilder.newBuilder()
            .maximumSize(10000).expireAfterAccess(5, TimeUnit.MINUTES).<UUID, PgPointsRepository.Balance>build().asMap();
    private final Object readLock = new Object();
    private final Map<UUID, CompletableFuture<Integer>> loading = new HashMap<>();
    private final LinkedHashSet<UUID> pending = new LinkedHashSet<>();
    private final Consumer<Throwable> errors;
    private final AtomicLong singleReads = new AtomicLong();
    private final AtomicLong batchReads = new AtomicLong();
    private boolean reconciling;
    private volatile boolean closed;

    public PgPointsWallet(PgPointsRepository repository) { this(repository, e -> { }); }
    public PgPointsWallet(PgPointsRepository repository, Consumer<Throwable> errors) {
        this.repository = repository;
        this.errors = errors;
    }

    private static ThreadPoolExecutor executor(String name, int capacity) {
        return new ThreadPoolExecutor(1, 1, 0, TimeUnit.SECONDS, new ArrayBlockingQueue<>(capacity), r -> {
            Thread t = new Thread(r, name); t.setDaemon(true); return t;
        }, new ThreadPoolExecutor.AbortPolicy());
    }

    private <T> CompletableFuture<T> write(Callable<T> operation) {
        CompletableFuture<T> future = new CompletableFuture<>();
        try {
            if (closed) throw new RejectedExecutionException("PlayerPoints wallet closed");
            writer.execute(() -> {
                try { future.complete(operation.call()); }
                catch (Throwable e) { future.completeExceptionally(e); }
            });
        } catch (RejectedExecutionException e) { future.completeExceptionally(e); }
        return future;
    }

    private void publish(UUID id, PgPointsRepository.Balance value) {
        cache.compute(id, (k, old) -> old == null || value.revision >= old.revision ? value : old);
    }

    public boolean cached(UUID id) { return cache.containsKey(id); }
    public long cachedRevision(UUID id) {
        PgPointsRepository.Balance value = cache.get(id);
        return value == null ? -1 : value.revision;
    }

    /** Coalesce overlapping explicit loads. SQL runs only in the display reader. */
    public CompletableFuture<Integer> refresh(UUID id) {
        synchronized (readLock) {
            CompletableFuture<Integer> existing = loading.get(id);
            if (existing != null) return existing;
            CompletableFuture<Integer> future = new CompletableFuture<>();
            loading.put(id, future);
            try {
                if (closed) throw new RejectedExecutionException("PlayerPoints wallet closed");
                reader.execute(() -> {
                    try {
                        singleReads.incrementAndGet();
                        PgPointsRepository.Balance value = repository.read(id);
                        publish(id, value);
                        synchronized (readLock) { loading.remove(id, future); }
                        future.complete(value.points);
                    } catch (Throwable e) {
                        synchronized (readLock) { loading.remove(id, future); }
                        future.completeExceptionally(e);
                    }
                });
            } catch (RejectedExecutionException e) {
                loading.remove(id, future);
                future.completeExceptionally(e);
            }
            return future;
        }
    }

    public int balance(UUID id) {
        PgPointsRepository.Balance value = cache.get(id);
        return value == null ? await(refresh(id)) : value.points;
    }

    /** Merge notifications and fallback snapshots; at most one reconciliation task is queued/running. */
    public void reconcile(Collection<UUID> ids) {
        synchronized (readLock) {
            if (closed) return;
            for (UUID id : ids) {
                if (pending.size() >= 10000) break;
                pending.add(id);
            }
            if (!pending.isEmpty() && !reconciling) {
                reconciling = true;
                scheduleBatch();
            }
        }
    }

    private void scheduleBatch() {
        try { reader.execute(this::readBatch); }
        catch (RejectedExecutionException e) {
            reconciling = false;
            // Retain pending UUIDs. The next timer/reconnect retries display reconciliation, never transactions.
            if (!closed) errors.accept(e);
        }
    }

    private void readBatch() {
        List<UUID> ids = new ArrayList<>(256);
        synchronized (readLock) {
            Iterator<UUID> iterator = pending.iterator();
            while (iterator.hasNext() && ids.size() < 256) { ids.add(iterator.next()); iterator.remove(); }
        }
        try {
            if (!ids.isEmpty()) {
                batchReads.incrementAndGet();
                repository.readMany(ids).forEach((id, value) -> cache.computeIfPresent(id,
                        (k, old) -> value.revision >= old.revision ? value : old));
            }
        } catch (Throwable e) { errors.accept(e); }
        finally {
            synchronized (readLock) {
                if (!closed && !pending.isEmpty()) scheduleBatch();
                else reconciling = false;
            }
        }
    }

    public CompletableFuture<Boolean> change(UUID id, PendingTransaction transaction) {
        return write(() -> {
            PgPointsRepository.Balance value = repository.change(id, transaction);
            if (value == null) return false;
            publish(id, value); return true;
        });
    }

    public CompletableFuture<Boolean> transfer(UUID from, UUID to, PendingTransaction debit, PendingTransaction credit) {
        return write(() -> {
            Map<UUID, PgPointsRepository.Balance> values = repository.transfer(from, to, debit, credit);
            if (values == null) return false;
            values.forEach(this::publish); return true;
        });
    }

    public void refreshCached(Consumer<Throwable> error) { reconcile(new ArrayList<>(cache.keySet())); }

    public CompletableFuture<Void> offsetAll(int amount) {
        return write(() -> { repository.offsetAll(amount).forEach(this::publish); return null; });
    }

    public void clear() { cache.clear(); }

    public Map<String, Long> diagnostics() {
        Map<String, Long> result = new LinkedHashMap<>();
        result.put("singleReads", singleReads.get()); result.put("batchReads", batchReads.get());
        result.put("writeQueue", (long) writer.getQueue().size()); result.put("readQueue", (long) reader.getQueue().size());
        synchronized (readLock) { result.put("pendingAccounts", (long) pending.size()); }
        return result;
    }

    public static <T> T await(CompletableFuture<T> future) {
        try { return future.get(30, TimeUnit.SECONDS); }
        catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new IllegalStateException("Points operation interrupted; outcome unknown", e); }
        catch (ExecutionException | TimeoutException e) { throw new IllegalStateException("Points operation failed; reconcile before retry", e); }
    }

    @Override public void close() {
        closed = true;
        writer.shutdown(); reader.shutdown();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        try {
            if (!writer.awaitTermination(30, TimeUnit.SECONDS)
                    || !reader.awaitTermination(Math.max(0, deadline - System.nanoTime()), TimeUnit.NANOSECONDS))
                throw new IllegalStateException("PlayerPoints PG shutdown still has pending operations");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt(); throw new IllegalStateException(e);
        }
    }
}
