package org.black_ixx.playerpoints.storage;

import org.junit.*;
import static org.junit.Assert.*;
import java.sql.*;
import javax.sql.DataSource;
import java.io.PrintWriter;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.Logger;
import org.black_ixx.playerpoints.models.*;

/** Real PG only. Absent env skips instead of claiming a mock verified concurrency. */
public class PgPointsRepositoryTest {
    private DataSource ds;
    private String prefix;
    private PgPointsRepository repo;
    @Before public void setup() throws Exception {
        Assume.assumeTrue(System.getenv("PLAYERPOINTS_TEST_PG_URL") != null);
        ds = new DataSource() {
            public Connection getConnection() throws SQLException {return DriverManager.getConnection(System.getenv("PLAYERPOINTS_TEST_PG_URL"),System.getenv("PLAYERPOINTS_TEST_PG_USER"),System.getenv("PLAYERPOINTS_TEST_PG_PASSWORD"));}
            public Connection getConnection(String u,String p)throws SQLException{return getConnection();}
            public PrintWriter getLogWriter(){return null;}public void setLogWriter(PrintWriter p){}public void setLoginTimeout(int n){}public int getLoginTimeout(){return 0;}
            public Logger getParentLogger(){return Logger.getGlobal();}public <T>T unwrap(Class<T> c)throws SQLException{throw new SQLException();}public boolean isWrapperFor(Class<?> c){return false;}
        };
        prefix="pptest_"+UUID.randomUUID().toString().replace("-","")+"_";
        repo=new PgPointsRepository(ds,prefix,0,true);repo.initialize();
    }
    @After public void cleanup() throws Exception {
        if(ds==null)return;
        try(Connection c=ds.getConnection();Statement s=c.createStatement()){
            for(String name:Arrays.asList("points","username_cache","transaction_log","pg_schema"))s.execute("DROP TABLE IF EXISTS "+prefix+name+" CASCADE");
            for(String name:Arrays.asList("bump_revision","notify_points"))s.execute("DROP FUNCTION IF EXISTS "+prefix+name+"() CASCADE");
        }
    }
    private PendingTransaction change(int amount){return new PendingTransaction(UpdateType.OFFSET,TransactionType.OFFSET,"Test",null,amount);}
    private void set(UUID id,int value)throws Exception{assertNotNull(repo.change(id,new PendingTransaction(UpdateType.SET,TransactionType.SET,"Set",null,value)));}
    private <T> List<T> parallel(int n,Callable<T> f)throws Exception{
        ExecutorService e=Executors.newFixedThreadPool(8);List<Future<T>> jobs=new ArrayList<>();
        try{for(int i=0;i<n;i++)jobs.add(e.submit(f));List<T> r=new ArrayList<>();for(Future<T> j:jobs)r.add(j.get(30,TimeUnit.SECONDS));return r;}finally{e.shutdownNow();}
    }
    private int scalar(String sql)throws Exception{try(Connection c=ds.getConnection();Statement s=c.createStatement();ResultSet r=s.executeQuery(sql)){r.next();return r.getInt(1);}}
    @Test public void simultaneousSchemaInitializationIsIdempotent()throws Exception{
        parallel(16,()->{new PgPointsRepository(ds,prefix,0,true).initialize();return true;});assertEquals(1,scalar("SELECT COUNT(*) FROM "+prefix+"pg_schema"));
    }
    @Test public void concurrentMissingAccountGetsStartingBalanceOnce()throws Exception{
        PgPointsRepository r=new PgPointsRepository(ds,prefix,17,true);UUID id=UUID.randomUUID();
        for(Integer value:parallel(64,()->r.read(id).points))assertEquals(17,value.intValue());
        assertEquals(1,scalar("SELECT COUNT(*) FROM "+prefix+"points"));
    }
    @Test public void eightConnectionsCannotOverspend()throws Exception{
        UUID id=UUID.randomUUID();set(id,100);int accepted=0;
        for(Boolean b:parallel(400,()->repo.change(id,change(-1))!=null))if(b)accepted++;
        assertEquals(100,accepted);assertEquals(0,repo.read(id).points);assertEquals(101,scalar("SELECT COUNT(*) FROM "+prefix+"transaction_log"));
    }
    @Test public void twoRepositoryInstancesKeepAllConcurrentOffsets()throws Exception{
        UUID id=UUID.randomUUID();set(id,10000);PgPointsRepository other=new PgPointsRepository(ds,prefix,0,true);
        parallel(400,()->{assertNotNull(repo.change(id,change(10)));assertNotNull(other.change(id,change(-5)));return true;});assertEquals(12000,repo.read(id).points);assertEquals(801,repo.read(id).revision);
    }
    @Test public void oppositeTransfersDoNotDeadlockOrLoseTotal()throws Exception{
        UUID a=UUID.randomUUID(),b=UUID.randomUUID();set(a,1000);set(b,1000);
        parallel(200,()->{
            assertNotNull(repo.transfer(a,b,change(-1),change(1)));
            assertNotNull(repo.transfer(b,a,change(-1),change(1)));return true;
        });assertEquals(1000,repo.read(a).points);assertEquals(1000,repo.read(b).points);
    }
    @Test public void overflowAndInsufficientTransfersAreRejectedWithoutPartialWrites()throws Exception{
        UUID a=UUID.randomUUID(),b=UUID.randomUUID();set(a,3);set(b,Integer.MAX_VALUE);
        assertNull(repo.change(b,change(1)));assertNull(repo.change(a,change(-4)));
        assertNull(repo.transfer(a,b,change(-1),change(1)));assertEquals(3,repo.read(a).points);assertEquals(Integer.MAX_VALUE,repo.read(b).points);
    }
    @Test public void logFailureRollsBackBalanceAndRevision()throws Exception{
        UUID id=UUID.randomUUID();set(id,50);long revision=repo.read(id).revision;
        try(Connection c=ds.getConnection();Statement s=c.createStatement()){s.execute("ALTER TABLE "+prefix+"transaction_log ADD CHECK(description <> 'Test')");}
        try{repo.change(id,change(-10));fail("log must fail");}catch(SQLException expected){}
        assertEquals(50,repo.read(id).points);assertEquals(revision,repo.read(id).revision);
    }
    @Test public void failedTransferLogRollsBackBothAccounts()throws Exception{
        UUID a=UUID.randomUUID(),b=UUID.randomUUID();set(a,20);set(b,30);
        try(Connection c=ds.getConnection();Statement s=c.createStatement()){s.execute("ALTER TABLE "+prefix+"transaction_log ADD CHECK(description <> 'Test')");}
        try{repo.transfer(a,b,change(-10),change(10));fail("log must fail");}catch(SQLException expected){}
        assertEquals(20,repo.read(a).points);assertEquals(30,repo.read(b).points);
    }
    @Test public void bulkOffsetClampsZeroAndCommitsAllBalancesAndLogs() throws Exception {
        UUID a=UUID.randomUUID(),b=UUID.randomUUID();set(a,5);set(b,20);
        repo.offsetAll(-10);assertEquals(0,repo.read(a).points);assertEquals(10,repo.read(b).points);
        assertEquals(4,scalar("SELECT COUNT(*) FROM "+prefix+"transaction_log"));
    }
    @Test public void bulkOverflowRollsBackAllAccounts() throws Exception {
        UUID a=UUID.randomUUID(),b=UUID.randomUUID();set(a,5);set(b,Integer.MAX_VALUE);
        try{repo.offsetAll(1);fail("bulk overflow must fail");}catch(SQLException expected){}
        assertEquals(5,repo.read(a).points);assertEquals(Integer.MAX_VALUE,repo.read(b).points);
        assertEquals(2,scalar("SELECT COUNT(*) FROM "+prefix+"transaction_log"));
    }
}
