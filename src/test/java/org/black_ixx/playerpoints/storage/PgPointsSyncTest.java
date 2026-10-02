package org.black_ixx.playerpoints.storage;

import java.io.PrintWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.logging.Logger;
import javax.sql.DataSource;
import org.black_ixx.playerpoints.manager.PgPointsSyncManager;
import org.black_ixx.playerpoints.models.PendingTransaction;
import org.black_ixx.playerpoints.models.TransactionType;
import org.black_ixx.playerpoints.models.UpdateType;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.postgresql.PGConnection;
import org.postgresql.PGNotification;
import static org.junit.Assert.*;

/** No mock SQL: transaction notifications, connection failure and stale query races use a real PG server. */
public class PgPointsSyncTest {
    private String prefix;
    private PgPointsRepository repo;
    private final List<AutoCloseable> resources = new ArrayList<>();

    private Connection connection() throws SQLException {
        return DriverManager.getConnection(System.getenv("PLAYERPOINTS_TEST_PG_URL"),
                System.getenv("PLAYERPOINTS_TEST_PG_USER"), System.getenv("PLAYERPOINTS_TEST_PG_PASSWORD"));
    }
    private DataSource source() {
        return new DataSource() {
            public Connection getConnection() throws SQLException { return connection(); }
            public Connection getConnection(String u, String p) throws SQLException { return connection(); }
            public PrintWriter getLogWriter() { return null; } public void setLogWriter(PrintWriter p) { }
            public void setLoginTimeout(int n) { } public int getLoginTimeout() { return 0; }
            public Logger getParentLogger() { return Logger.getGlobal(); }
            public <T> T unwrap(Class<T> c) throws SQLException { throw new SQLException(); }
            public boolean isWrapperFor(Class<?> c) { return false; }
        };
    }
    @Before public void setup() throws Exception {
        Assume.assumeTrue(System.getenv("PLAYERPOINTS_TEST_PG_URL") != null);
        prefix = "ppsync_" + UUID.randomUUID().toString().replace("-", "") + "_";
        repo = new PgPointsRepository(source(), prefix, 0, true); repo.initialize();
    }
    @After public void cleanup() throws Exception {
        for (int i = resources.size() - 1; i >= 0; i--) resources.get(i).close();
        if (prefix == null) return;
        try (Connection c = connection(); Statement s = c.createStatement()) {
            for (String name : Arrays.asList("points", "username_cache", "transaction_log", "pg_schema"))
                s.execute("DROP TABLE IF EXISTS " + prefix + name + " CASCADE");
            for (String name : Arrays.asList("bump_revision", "notify_points"))
                s.execute("DROP FUNCTION IF EXISTS " + prefix + name + "() CASCADE");
        }
    }
    private <T extends AutoCloseable> T own(T r) { resources.add(r); return r; }
    private PendingTransaction delta(int n) { return new PendingTransaction(UpdateType.OFFSET, TransactionType.OFFSET, "SyncTest", null, n); }
    private void set(UUID id, int n) throws Exception { repo.change(id, new PendingTransaction(UpdateType.SET, TransactionType.SET, "Set", null, n)); }
    private void until(BooleanSupplier predicate) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(12);
        while (!predicate.getAsBoolean()) {
            if (System.nanoTime() > deadline) fail("Condition timed out");
            Thread.sleep(20);
        }
    }
    private PGConnection listening(Connection c) throws Exception {
        c.setAutoCommit(true);
        try (Statement s = c.createStatement()) { s.execute("LISTEN " + repo.notificationChannel()); }
        return c.unwrap(PGConnection.class);
    }

    @Test public void notificationsOnlyDescribeCommittedChanges() throws Exception {
        UUID a = UUID.randomUUID(), b = UUID.randomUUID(); set(a, 20); set(b, 30);
        try (Connection c = connection()) {
            PGConnection pg = listening(c);
            assertNull(repo.change(a, delta(-21)));
            assertEquals(0, pg.getNotifications(100).length);
            assertNotNull(repo.transfer(a, b, delta(-2), delta(2)));
            PGNotification[] messages = pg.getNotifications(3000);
            assertEquals(2, messages.length);
            assertEquals(new java.util.HashSet<>(Arrays.asList(a.toString(), b.toString())),
                    new java.util.HashSet<>(Arrays.asList(messages[0].getParameter().split(":")[0], messages[1].getParameter().split(":")[0])));
            try (Connection bad = connection(); Statement s = bad.createStatement()) {
                s.execute("ALTER TABLE " + prefix + "transaction_log ADD CHECK(description <> 'SyncTest') NOT VALID");
            }
            try { repo.change(a, delta(1)); fail("log constraint must roll back"); } catch (SQLException expected) { }
            assertEquals(0, pg.getNotifications(100).length);
            assertEquals(18, repo.read(a).points);
        }
    }

    @Test public void migrationPreservesExistingBalancesAndExternalWritesBumpRevision() throws Exception {
        UUID id = UUID.randomUUID(); set(id, 42); long old = repo.read(id).revision;
        try (Connection c = connection(); Statement s = c.createStatement()) {
            s.execute("DROP TRIGGER points_revision ON " + prefix + "points");
            s.execute("DROP TRIGGER points_notify ON " + prefix + "points");
            s.execute("DROP FUNCTION " + prefix + "bump_revision()");
            s.execute("DROP FUNCTION " + prefix + "notify_points()");
            s.execute("UPDATE " + prefix + "pg_schema SET version=1");
        }
        repo.initialize(); assertEquals(42, repo.read(id).points);
        try (Connection c = connection(); Statement s = c.createStatement()) {
            s.execute("UPDATE " + prefix + "points SET points=43 WHERE uuid='" + id + "'");
        }
        assertEquals(old + 1, repo.read(id).revision);
    }

    @Test public void batchReadsDoNotCreateAccountsOrWriteRevisions() throws Exception {
        UUID id = UUID.randomUUID(); set(id, 17); long revision = repo.read(id).revision;
        assertEquals(1, repo.readMany(Arrays.asList(id, UUID.randomUUID())).size());
        assertEquals(revision, repo.read(id).revision);
        try (Connection c = connection(); Statement s = c.createStatement(); ResultSet r = s.executeQuery("SELECT COUNT(*) FROM " + prefix + "points")) {
            r.next(); assertEquals(1, r.getInt(1));
        }
    }

    @Test public void twoWalletsUpdateFromNotificationsAndIgnoreMalformedPayloads() throws Exception {
        UUID id = UUID.randomUUID(); set(id, 100);
        PgPointsWallet a = own(new PgPointsWallet(repo)), b = own(new PgPointsWallet(repo));
        a.refresh(id).get(); b.refresh(id).get();
        List<Throwable> errors = new java.util.concurrent.CopyOnWriteArrayList<>();
        PgPointsSyncManager sa = own(new PgPointsSyncManager(a, this::connection, repo.notificationChannel(), 30, errors::add));
        PgPointsSyncManager sb = own(new PgPointsSyncManager(b, this::connection, repo.notificationChannel(), 30, errors::add));
        sa.join(id); sb.join(id);
        until(() -> sa.diagnostics().get("listenerConnected") == 1 && sb.diagnostics().get("listenerConnected") == 1);
        assertTrue(a.change(id, delta(-10)).get());
        until(() -> b.balance(id) == 90);
        try (Connection c = connection(); PreparedStatement s = c.prepareStatement("SELECT pg_notify(?,?)")) {
            s.setString(1, repo.notificationChannel()); s.setString(2, "invalid:999999"); s.execute();
        }
        assertEquals(90, b.balance(id));
        assertTrue(b.transfer(id, UUID.randomUUID(), delta(-5), delta(5)).get());
        until(() -> a.balance(id) == 85);
        assertEquals(85, repo.read(id).points);
        assertTrue(errors.toString(), errors.isEmpty());
        long reads = a.diagnostics().get("batchReads");
        long notices = sa.diagnostics().get("notifications");
        assertTrue(a.change(id, delta(1)).get());
        until(() -> sa.diagnostics().get("notifications") > notices && b.balance(id) == 86);
        Thread.sleep(200);
        assertEquals("Local commit must not trigger redundant display SQL", reads, a.diagnostics().get("batchReads").longValue());
    }

    @Test public void disconnectedListenerFallsBackAndReconnectsAcrossMissedChanges() throws Exception {
        UUID id = UUID.randomUUID(); set(id, 100);
        PgPointsWallet wallet = own(new PgPointsWallet(repo)); wallet.refresh(id).get();
        AtomicBoolean allow = new AtomicBoolean(true); AtomicReference<Connection> active = new AtomicReference<>();
        PgPointsSyncManager sync = own(new PgPointsSyncManager(wallet, () -> {
            if (!allow.get()) throw new SQLException("injected listener outage");
            Connection c = connection(); active.set(c); return c;
        }, repo.notificationChannel(), 5, e -> { }));
        sync.join(id); until(() -> sync.diagnostics().get("listenerConnected") == 1);
        allow.set(false); active.get().close();
        until(() -> sync.diagnostics().get("listenerConnected") == 0);
        repo.change(id, delta(10));
        until(() -> wallet.balance(id) == 110); // listener is still down; the batch fallback must work
        assertEquals(0L, sync.diagnostics().get("listenerConnected").longValue());
        repo.change(id, delta(20)); allow.set(true);
        until(() -> sync.diagnostics().get("subscriptions") >= 2 && wallet.balance(id) == 130);
        assertTrue(sync.diagnostics().get("notificationErrors") > 0);
    }

    @Test public void thousandIdleAccountsUseFourReadOnlyBatches() throws Exception {
        List<UUID> ids = new ArrayList<>();
        try (Connection c = connection(); PreparedStatement s = c.prepareStatement("INSERT INTO " + prefix + "points(uuid,points) VALUES(?,1)")) {
            for (int i = 0; i < 1000; i++) { UUID id = UUID.randomUUID(); ids.add(id); s.setString(1, id.toString()); s.addBatch(); }
            s.executeBatch();
        }
        PgPointsWallet wallet = own(new PgPointsWallet(repo));
        wallet.reconcile(ids);
        until(() -> wallet.diagnostics().get("batchReads") == 4 && wallet.diagnostics().get("pendingAccounts") == 0 && wallet.diagnostics().get("readQueue") == 0);
        assertEquals(0L, wallet.diagnostics().get("singleReads").longValue());
        try (Connection c = connection(); Statement s = c.createStatement(); ResultSet r = s.executeQuery("SELECT SUM(revision) FROM " + prefix + "points")) {
            r.next(); assertEquals(0, r.getLong(1));
        }
    }

    @Test public void blockedDisplayReadDoesNotBlockDebitOrRegressCacheAndDuplicateReadsCoalesce() throws Exception {
        UUID id = UUID.randomUUID(); set(id, 100);
        CountDownLatch snapshot = new CountDownLatch(1), release = new CountDownLatch(1);
        AtomicBoolean block = new AtomicBoolean(false); AtomicInteger queries = new AtomicInteger();
        DataSource proxy = new DataSource() {
            public Connection getConnection() throws SQLException {
                Connection real = connection();
                return (Connection) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[]{Connection.class}, (p,m,args) -> {
                    try {
                        Object result = m.invoke(real,args);
                        if (result instanceof PreparedStatement && ((String)args[0]).startsWith("SELECT uuid,points,revision")) {
                            PreparedStatement statement = (PreparedStatement) result;
                            return Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[]{PreparedStatement.class}, (p2,m2,a2) -> {
                                try {
                                    Object r = m2.invoke(statement,a2);
                                    if (m2.getName().equals("executeQuery") && block.compareAndSet(true,false)) {
                                        queries.incrementAndGet(); snapshot.countDown();
                                        if (!release.await(10, TimeUnit.SECONDS)) throw new SQLException("injected query barrier timed out");
                                    }
                                    return r;
                                } catch (InvocationTargetException e) { throw e.getCause(); }
                            });
                        }
                        return result;
                    } catch (InvocationTargetException e) { throw e.getCause(); }
                });
            }
            public Connection getConnection(String u,String p)throws SQLException{return getConnection();}
            public PrintWriter getLogWriter(){return null;}public void setLogWriter(PrintWriter p){}public void setLoginTimeout(int n){}public int getLoginTimeout(){return 0;}
            public Logger getParentLogger(){return Logger.getGlobal();}public <T>T unwrap(Class<T> c)throws SQLException{throw new SQLException();}public boolean isWrapperFor(Class<?> c){return false;}
        };
        PgPointsWallet wallet = own(new PgPointsWallet(new PgPointsRepository(proxy, prefix, 0, true)));
        wallet.refresh(id).get(); block.set(true);
        CompletableFuture<Integer> reading = wallet.refresh(id);
        assertTrue(snapshot.await(3, TimeUnit.SECONDS));
        try {
            for (int i=0;i<100;i++) assertSame(reading, wallet.refresh(id));
            assertTrue(wallet.change(id, delta(-10)).get(3, TimeUnit.SECONDS));
            assertEquals(90, wallet.balance(id));
        } finally { release.countDown(); }
        assertEquals(100, reading.get().intValue()); // old committed snapshot is deliberately late
        assertEquals(90, wallet.balance(id)); assertEquals(1, queries.get());
    }

    @Test public void closedWalletRejectsRequestsWithoutHangingFutures() throws Exception {
        PgPointsWallet wallet = new PgPointsWallet(repo); wallet.close();
        assertTrue(wallet.change(UUID.randomUUID(), delta(1)).isCompletedExceptionally());
        assertTrue(wallet.refresh(UUID.randomUUID()).isCompletedExceptionally());
    }
}
