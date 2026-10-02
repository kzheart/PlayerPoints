package org.black_ixx.playerpoints.storage;

import java.sql.*;
import java.util.*;
import javax.sql.DataSource;
import org.black_ixx.playerpoints.models.PendingTransaction;
import org.black_ixx.playerpoints.models.UpdateType;

/** Committed PG balances are authoritative; no Bukkit calls or process-local balance checks. */
public final class PgPointsRepository {
    public static final class Balance {
        public final int points;
        public final long revision;
        public Balance(int points, long revision) { this.points = points; this.revision = revision; }
    }
    private final DataSource source;
    private final String prefix;
    private final int startingBalance;
    private final boolean logging;

    public PgPointsRepository(DataSource source, String prefix, int startingBalance, boolean logging) {
        if (!prefix.matches("[a-z][a-z0-9_]{0,40}") || startingBalance < 0)
            throw new IllegalArgumentException("Invalid PG prefix or starting balance");
        this.source = source; this.prefix = prefix; this.startingBalance = startingBalance; this.logging = logging;
    }

    public void initialize() throws SQLException {
        try (Connection c = source.getConnection()) {
            c.setAutoCommit(false);
            try (Statement s = c.createStatement()) {
                // Serialize first boot/schema changes across game servers.
                try (PreparedStatement p = c.prepareStatement("SELECT pg_advisory_xact_lock(?)")) {
                    p.setLong(1, 0x50504d554c414eL); p.execute();
                }
                s.execute("CREATE TABLE IF NOT EXISTS " + prefix + "pg_schema (id INTEGER PRIMARY KEY CHECK(id=1), version INTEGER NOT NULL)");
                s.execute("INSERT INTO " + prefix + "pg_schema VALUES(1,0) ON CONFLICT(id) DO NOTHING");
                try (ResultSet r = s.executeQuery("SELECT version FROM " + prefix + "pg_schema WHERE id=1 FOR UPDATE")) {
                    r.next(); int v = r.getInt(1);
                    if (v > 2) throw new SQLException("Unsupported newer PlayerPoints PG schema");
                    if (v == 0) {
                        s.execute("CREATE TABLE IF NOT EXISTS " + prefix + "points (id BIGSERIAL PRIMARY KEY, uuid VARCHAR(36) UNIQUE NOT NULL, points INTEGER NOT NULL CHECK(points>=0), revision BIGINT NOT NULL DEFAULT 0)");
                        s.execute("CREATE TABLE IF NOT EXISTS " + prefix + "username_cache (uuid VARCHAR(36) PRIMARY KEY, username VARCHAR(30) NOT NULL)");
                        s.execute("CREATE INDEX IF NOT EXISTS " + prefix + "username_cache_username_index ON " + prefix + "username_cache(username)");
                        s.execute("CREATE TABLE IF NOT EXISTS " + prefix + "transaction_log (transaction_type VARCHAR(20) NOT NULL, description VARCHAR(100) NOT NULL, source VARCHAR(36), receiver VARCHAR(36) NOT NULL, amount INTEGER NOT NULL, timestamp TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP)");
                        s.execute("UPDATE " + prefix + "pg_schema SET version=1 WHERE id=1");
                    }
                }
                // Trigger covers all writers, including SQL maintenance tools. NOTIFY is delivered only after commit.
                if (schemaVersion(c) < 2) {
                    s.execute("CREATE OR REPLACE FUNCTION " + prefix + "bump_revision() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN NEW.revision := GREATEST(NEW.revision, OLD.revision + 1); RETURN NEW; END $$");
                    s.execute("CREATE TRIGGER points_revision BEFORE UPDATE ON " + prefix + "points FOR EACH ROW EXECUTE FUNCTION " + prefix + "bump_revision()");
                    s.execute("CREATE OR REPLACE FUNCTION " + prefix + "notify_points() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_notify('" + notificationChannel() + "', NEW.uuid || ':' || NEW.revision::text); RETURN NEW; END $$");
                    s.execute("CREATE TRIGGER points_notify AFTER INSERT OR UPDATE ON " + prefix + "points FOR EACH ROW EXECUTE FUNCTION " + prefix + "notify_points()");
                    s.execute("UPDATE " + prefix + "pg_schema SET version=2 WHERE id=1");
                }
                c.commit();
            } catch (SQLException | RuntimeException e) { c.rollback(); throw e; }
        }
    }

    public String notificationChannel() { return prefix + "changed"; }

    private int schemaVersion(Connection c) throws SQLException {
        try (Statement s = c.createStatement(); ResultSet r = s.executeQuery("SELECT version FROM " + prefix + "pg_schema WHERE id=1")) {
            r.next(); return r.getInt(1);
        }
    }

    private void ensure(Connection c, UUID id) throws SQLException {
        try (PreparedStatement s = c.prepareStatement("INSERT INTO " + prefix + "points(uuid,points) VALUES(?,?) ON CONFLICT(uuid) DO NOTHING")) {
            s.setString(1,id.toString()); s.setInt(2,startingBalance); s.executeUpdate();
        }
    }
    private Balance read(Connection c, UUID id, boolean lock) throws SQLException {
        try (PreparedStatement s = c.prepareStatement("SELECT points,revision FROM " + prefix + "points WHERE uuid=?" + (lock ? " FOR UPDATE" : ""))) {
            s.setString(1,id.toString());
            try (ResultSet r=s.executeQuery()) {
                if (!r.next()) throw new SQLException("Missing points account");
                return new Balance(r.getInt(1),r.getLong(2));
            }
        }
    }
    public Balance read(UUID id) throws SQLException {
        try (Connection c=source.getConnection()) {
            // Existing accounts require one SELECT, not an INSERT on every display read.
            Map<UUID, Balance> existing = readMany(c, Collections.singleton(id));
            if (existing.containsKey(id)) return existing.get(id);
            ensure(c,id); return read(c,id,false);
        }
    }
    /** Read-only cache reconciliation. Never creates an account or overwrites a balance. */
    public Map<UUID, Balance> readMany(Collection<UUID> ids) throws SQLException {
        if (ids.isEmpty()) return Collections.emptyMap();
        if (ids.size() > 256) throw new IllegalArgumentException("PG reconciliation batch exceeds 256 accounts");
        try (Connection c = source.getConnection()) { return readMany(c, ids); }
    }

    private Map<UUID, Balance> readMany(Connection c, Collection<UUID> ids) throws SQLException {
        String[] keys = ids.stream().map(UUID::toString).toArray(String[]::new);
        Array array = c.createArrayOf("varchar", keys);
        try (PreparedStatement s = c.prepareStatement("SELECT uuid,points,revision FROM " + prefix + "points WHERE uuid = ANY (?)")) {
            s.setArray(1, array);
            Map<UUID, Balance> result = new HashMap<>();
            try (ResultSet r = s.executeQuery()) {
                while (r.next()) result.put(UUID.fromString(r.getString(1)), new Balance(r.getInt(2), r.getLong(3)));
            }
            return result;
        } finally { array.free(); }
    }

    private Balance write(Connection c, UUID id, int value) throws SQLException {
        try (PreparedStatement s=c.prepareStatement("UPDATE " + prefix + "points SET points=?,revision=revision+1 WHERE uuid=? RETURNING points,revision")) {
            s.setInt(1,value); s.setString(2,id.toString());
            try(ResultSet r=s.executeQuery()) { if(!r.next())throw new SQLException("Missing points account"); return new Balance(r.getInt(1),r.getLong(2)); }
        }
    }
    private void log(Connection c, UUID id, PendingTransaction t) throws SQLException {
        if(!logging)return;
        try(PreparedStatement s=c.prepareStatement("INSERT INTO " + prefix + "transaction_log(transaction_type,description,source,receiver,amount) VALUES(?,?,?,?,?)")) {
            s.setString(1,t.getTransactionType().name()); s.setString(2,t.getSourceDescription());
            s.setString(3,t.getSource()==null?null:t.getSource().toString()); s.setString(4,id.toString()); s.setInt(5,t.getAmount()); s.executeUpdate();
        }
    }
    /** null means rejected with no committed mutation. Exceptions have an unknown outcome: never blindly replay. */
    public Balance change(UUID id, PendingTransaction t) throws SQLException {
        try(Connection c=source.getConnection()) {
            c.setAutoCommit(false);
            try {
                ensure(c,id); Balance b=read(c,id,true);
                long next=t.getUpdateType()==UpdateType.SET?t.getAmount():(long)b.points+t.getAmount();
                if(next<0 || next>Integer.MAX_VALUE){c.rollback();return null;}
                Balance value=write(c,id,(int)next); log(c,id,t); c.commit(); return value;
            } catch(SQLException|RuntimeException e){c.rollback();throw e;}
        }
    }
    /** Both accounts, both logs and both revisions commit together. Sorted locks avoid opposite-pay deadlocks. */
    public Map<UUID,Balance> transfer(UUID from, UUID to, PendingTransaction debit, PendingTransaction credit) throws SQLException {
        if(from.equals(to) || debit.getAmount()>=0 || credit.getAmount()<=0)return null;
        try(Connection c=source.getConnection()) {
            c.setAutoCommit(false);
            try {
                List<UUID> ids=new ArrayList<>(Arrays.asList(from,to)); ids.sort(Comparator.comparing(UUID::toString));
                Map<UUID,Balance> locked=new HashMap<>();
                for(UUID id:ids){ensure(c,id);locked.put(id,read(c,id,true));}
                long a=(long)locked.get(from).points+debit.getAmount();
                long b=(long)locked.get(to).points+credit.getAmount();
                if(a<0 || b>Integer.MAX_VALUE){c.rollback();return null;}
                Map<UUID,Balance> values=new HashMap<>();
                values.put(from,write(c,from,(int)a));values.put(to,write(c,to,(int)b));
                log(c,from,debit);log(c,to,credit);c.commit();return values;
            }catch(SQLException|RuntimeException e){c.rollback();throw e;}
        }
    }
    /** Existing-account bulk offset, serialized with ordinary debits by the same sorted row locks. */
    public Map<UUID,Balance> offsetAll(int amount) throws SQLException {
        try (Connection c = source.getConnection()) {
            c.setAutoCommit(false);
            try {
                Map<UUID,Integer> old = new LinkedHashMap<>();
                try (Statement s = c.createStatement(); ResultSet r = s.executeQuery("SELECT uuid,points FROM " + prefix + "points ORDER BY uuid FOR UPDATE")) {
                    while (r.next()) old.put(UUID.fromString(r.getString(1)), r.getInt(2));
                }
                Map<UUID,Balance> values = new HashMap<>();
                for (Map.Entry<UUID,Integer> e : old.entrySet()) {
                    long next = Math.max(0L, (long)e.getValue() + amount);
                    if (next > Integer.MAX_VALUE) throw new SQLException("Bulk offset exceeds points range; entire operation rolled back");
                    values.put(e.getKey(), write(c, e.getKey(), (int)next));
                    log(c, e.getKey(), new PendingTransaction(UpdateType.OFFSET,
                            org.black_ixx.playerpoints.models.TransactionType.OFFSET, "Give all", null, (int)next - e.getValue()));
                }
                c.commit(); return values;
            } catch (SQLException | RuntimeException e) { c.rollback(); throw e; }
        }
    }
}
