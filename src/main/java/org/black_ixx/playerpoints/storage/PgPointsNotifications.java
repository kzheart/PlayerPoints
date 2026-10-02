package org.black_ixx.playerpoints.storage;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.postgresql.PGConnection;
import org.postgresql.PGNotification;

/** A dedicated physical session. Each LISTEN is committed before the caller reconciles the missed interval. */
public final class PgPointsNotifications implements AutoCloseable {
    @FunctionalInterface public interface ConnectionFactory { Connection open() throws SQLException; }
    public static final class Change {
        public final UUID id;
        public final long revision;
        private Change(UUID id, long revision) { this.id = id; this.revision = revision; }
    }
    private final ConnectionFactory connections;
    private final String channel;
    private final Consumer<Change> changed;
    private final Runnable subscribed;
    private final Consumer<Throwable> errors;
    private final Thread thread;
    private volatile boolean closed;
    private volatile boolean connected;
    private volatile Connection active;
    private volatile long subscriptions;

    public PgPointsNotifications(ConnectionFactory connections, String channel, Consumer<Change> changed,
                                 Runnable subscribed, Consumer<Throwable> errors) {
        if (!channel.matches("[a-z][a-z0-9_]{0,62}")) throw new IllegalArgumentException("Invalid notification channel");
        this.connections = connections; this.channel = channel; this.changed = changed;
        this.subscribed = subscribed; this.errors = errors;
        this.thread = new Thread(this::listen, "PlayerPoints-PG-listen");
        thread.setDaemon(true);
    }

    public void start() { thread.start(); }
    public boolean connected() { return connected; }
    public long subscriptions() { return subscriptions; }

    private void listen() {
        long retryMillis = 1000;
        while (!closed) {
            try (Connection c = connections.open()) {
                active = c;
                if (closed) break;
                c.setAutoCommit(true);
                try (Statement s = c.createStatement()) { s.execute("LISTEN " + channel); }
                PGConnection pg = c.unwrap(PGConnection.class);
                connected = true; subscriptions++;
                subscribed.run();
                retryMillis = 1000;
                long heartbeat = System.nanoTime();
                while (!closed) {
                    PGNotification[] messages = pg.getNotifications(1000);
                    if (messages != null) for (PGNotification message : messages) {
                        if (!channel.equals(message.getName())) continue;
                        try {
                            String payload = message.getParameter();
                            int separator = payload.indexOf(':');
                            if (separator != 36 || payload.length() > 57) continue;
                            String key = payload.substring(0, separator);
                            UUID id = UUID.fromString(key);
                            long revision = Long.parseLong(payload.substring(separator + 1));
                            if (id.toString().equals(key) && revision >= 0) changed.accept(new Change(id, revision));
                        } catch (IllegalArgumentException ignored) { /* malformed invalidations do not mutate cache */ }
                    }
                    // Detect a half-open/idle connection; never hold an SQL transaction while listening.
                    if (System.nanoTime() - heartbeat >= TimeUnit.SECONDS.toNanos(30)) {
                        try (Statement s = c.createStatement()) { s.execute("SELECT 1"); }
                        heartbeat = System.nanoTime();
                    }
                }
            } catch (Exception e) { if (!closed) errors.accept(e); }
            finally { active = null; connected = false; }
            if (!closed) {
                try { Thread.sleep(retryMillis); }
                catch (InterruptedException e) { if (closed) break; }
                retryMillis = Math.min(30000, retryMillis * 2);
            }
        }
    }

    @Override public void close() {
        closed = true;
        Connection c = active;
        if (c != null) try { c.close(); } catch (SQLException ignored) { }
        thread.interrupt();
        try {
            thread.join(7000);
            if (thread.isAlive()) throw new IllegalStateException("PlayerPoints notification listener did not stop");
        } catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new IllegalStateException(e); }
    }
}
