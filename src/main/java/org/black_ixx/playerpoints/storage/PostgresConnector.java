package org.black_ixx.playerpoints.storage;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import dev.rosewood.rosegarden.database.DatabaseConnector;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import org.postgresql.Driver;

/** RoseGarden-compatible connector. Callback errors propagate, rather than being silently treated as success. */
public final class PostgresConnector implements DatabaseConnector {
    private final HikariDataSource pool;
    private final String url;
    private final Properties listenerProperties = new Properties();
    private final Object lock = new Object();
    public PostgresConnector(String host, int port, String database, String user, String password, String sslmode, int size) {
        if (!host.matches("[a-zA-Z0-9.:-]+") || !database.matches("[a-zA-Z0-9_-]+") || port<1 || port>65535 || size<2 || size>32 ||
            !java.util.Arrays.asList("disable","allow","prefer","require","verify-ca","verify-full").contains(sslmode))
            throw new IllegalArgumentException("Invalid PostgreSQL settings");
        HikariConfig c = new HikariConfig();
        this.url = "jdbc:postgresql://"+host+":"+port+"/"+database;
        listenerProperties.setProperty("user", user);
        listenerProperties.setProperty("password", password);
        listenerProperties.setProperty("sslmode", sslmode);
        listenerProperties.setProperty("connectTimeout", "5");
        listenerProperties.setProperty("socketTimeout", "15");
        listenerProperties.setProperty("tcpKeepAlive", "true");
        listenerProperties.setProperty("ApplicationName", "PlayerPoints-sync");
        c.setJdbcUrl(url);
        c.setDriverClassName("org.postgresql.Driver"); c.setUsername(user); c.setPassword(password);
        c.addDataSourceProperty("sslmode",sslmode);c.addDataSourceProperty("connectTimeout","5");
        c.addDataSourceProperty("socketTimeout","15");c.addDataSourceProperty("options","-c statement_timeout=10000 -c lock_timeout=5000");
        c.setConnectionTimeout(5000);c.setMaximumPoolSize(size);c.setMinimumIdle(1);c.setPoolName("PlayerPoints-PG");
        pool=new HikariDataSource(c);
    }
    /** A physical session, outside the transaction pool; LISTEN never monopolizes a wallet connection. */
    public Connection openListener() throws SQLException { return new Driver().connect(url, listenerProperties); }
    public HikariDataSource source(){return pool;}
    @Override public Connection connect() throws SQLException {return pool.getConnection();}
    @Override public void connect(ConnectionCallback callback){connect(callback,false);}
    @Override public void connect(ConnectionCallback callback,boolean transaction){
        try(Connection c=connect()){
            if(transaction)c.setAutoCommit(false);
            try{callback.accept(c);if(transaction)c.commit();}
            catch(SQLException|RuntimeException e){if(transaction)c.rollback();throw e;}
        }catch(SQLException e){throw new IllegalStateException("PostgreSQL operation failed",e);}
    }
    @Override public Object getLock(){return lock;}
    @Override public boolean isFinished(){return pool.getHikariPoolMXBean().getActiveConnections()==0;}
    @Override public void cleanup(){}
    @Override public void closeConnection(){pool.close();}
}
