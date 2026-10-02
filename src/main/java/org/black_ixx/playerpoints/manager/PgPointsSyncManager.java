package org.black_ixx.playerpoints.manager;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import org.black_ixx.playerpoints.storage.PgPointsNotifications;
import org.black_ixx.playerpoints.storage.PgPointsWallet;

/** Notifications are hints; read-only batches and revision checks reconcile authoritative display state. */
public final class PgPointsSyncManager implements AutoCloseable {
    private final PgPointsWallet wallet;
    private final Set<UUID> online = Collections.newSetFromMap(new ConcurrentHashMap<UUID, Boolean>());
    private final ConcurrentHashMap<UUID, Long> dirty = new ConcurrentHashMap<>();
    private final ScheduledExecutorService timer = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "PlayerPoints-PG-sync"); t.setDaemon(true); return t;
    });
    private final PgPointsNotifications listener;
    private final AtomicLong notifications = new AtomicLong();
    private final AtomicLong notificationErrors = new AtomicLong();
    private final int intervalSeconds;

    public PgPointsSyncManager(PgPointsWallet wallet, PgPointsNotifications.ConnectionFactory connections,
                               String channel, int intervalSeconds, Consumer<Throwable> errors) {
        if (intervalSeconds < 5 || intervalSeconds > 3600) throw new IllegalArgumentException("PG reconciliation interval must be 5-3600 seconds");
        this.wallet = wallet; this.intervalSeconds = intervalSeconds;
        this.listener = new PgPointsNotifications(connections, channel, change -> {
            notifications.incrementAndGet();
            // Never retain arbitrary offline accounts; notifications cannot load millions of wallet entries.
            if ((online.contains(change.id) || wallet.cached(change.id)) && wallet.cachedRevision(change.id) < change.revision)
                dirty.merge(change.id, change.revision, Math::max);
        }, () -> wallet.reconcile(new ArrayList<>(online)), e -> {
            notificationErrors.incrementAndGet(); errors.accept(e);
        });
        timer.scheduleWithFixedDelay(() -> {
            ArrayList<UUID> ids = new ArrayList<>();
            dirty.forEach((id, revision) -> {
                if (dirty.remove(id, revision) && wallet.cachedRevision(id) < revision) ids.add(id);
            });
            wallet.reconcile(ids);
        }, 100, 100, TimeUnit.MILLISECONDS);
        timer.scheduleWithFixedDelay(() -> wallet.reconcile(new ArrayList<>(online)),
                intervalSeconds, intervalSeconds, TimeUnit.SECONDS);
        listener.start();
    }

    /** Main thread supplies player IDs; background tasks never touch Bukkit player objects. */
    public void join(UUID id) { online.add(id); wallet.reconcile(Collections.singleton(id)); }
    public void quit(UUID id) { online.remove(id); dirty.remove(id); }

    public Map<String, Long> diagnostics() {
        Map<String, Long> result = new LinkedHashMap<>(wallet.diagnostics());
        result.put("onlineAccounts", (long) online.size()); result.put("dirtyAccounts", (long) dirty.size());
        result.put("notifications", notifications.get()); result.put("notificationErrors", notificationErrors.get());
        result.put("listenerConnected", listener.connected() ? 1L : 0L);
        result.put("subscriptions", listener.subscriptions()); result.put("reconcileSeconds", (long) intervalSeconds);
        return result;
    }

    @Override public void close() { timer.shutdownNow(); listener.close(); dirty.clear(); online.clear(); }
}
