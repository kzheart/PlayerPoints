import java.io.PrintWriter;
import java.lang.management.ManagementFactory;
import java.lang.reflect.*;
import java.nio.file.*;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.Consumer;
import java.util.logging.Logger;
import javax.sql.DataSource;
import org.black_ixx.playerpoints.libs.hikari.HikariConfig;
import org.black_ixx.playerpoints.libs.hikari.HikariDataSource;
import org.black_ixx.playerpoints.models.PendingTransaction;
import org.black_ixx.playerpoints.models.TransactionType;
import org.black_ixx.playerpoints.models.UpdateType;

/** A/B actual wallet JARs. Bukkit/game/TPS are deliberately outside this storage-pipeline benchmark. */
public final class WalletBenchmark {
    static final UUID[] IDS = new UUID[1000];
    static { for (int i=0;i<IDS.length;i++) IDS[i]=UUID.nameUUIDFromBytes(("PPAB"+i).getBytes(java.nio.charset.StandardCharsets.UTF_8)); }
    static final AtomicBoolean measuring = new AtomicBoolean();
    static final ConcurrentHashMap<String,AtomicLong> sql = new ConcurrentHashMap<>();
    static final Queue<String> errors = new ConcurrentLinkedQueue<>();
    static final Queue<Double> latency = new ConcurrentLinkedQueue<>(), refreshLatency = new ConcurrentLinkedQueue<>(), visibilityLatency = new ConcurrentLinkedQueue<>(), schedulerDelay = new ConcurrentLinkedQueue<>();
    static final Queue<Visibility> waitingVisibility = new ConcurrentLinkedQueue<>();
    static final Queue<CompletableFuture<?>> pending = new ConcurrentLinkedQueue<>();
    static final AtomicInteger outstanding = new AtomicInteger(), maxOutstanding = new AtomicInteger(), maxQueue = new AtomicInteger(), maxConnections = new AtomicInteger();
    static final AtomicInteger accepted = new AtomicInteger(), rejected = new AtomicInteger(), writeErrors = new AtomicInteger(), refreshErrors = new AtomicInteger(), refreshSubmitted = new AtomicInteger();
    static final AtomicIntegerArray expected = new AtomicIntegerArray(1000);
    static Node[] nodes;
    static volatile long began;

    static void count(String statement) {
        if (!measuring.get()) return;
        String key;
        if (statement.startsWith("SELECT") && statement.contains("points") && statement.contains("FOR UPDATE")) key="transactionSelects";
        else if (statement.startsWith("SELECT") && statement.contains("points")) key=statement.contains("ANY")?"displayBatchSelects":"displaySingleSelects";
        else if (statement.startsWith("INSERT") && statement.contains("transaction_log")) key="logInserts";
        else if (statement.startsWith("INSERT") && statement.contains("points(")) key="accountEnsures";
        else if (statement.startsWith("UPDATE") && statement.contains("points SET")) key="balanceUpdates";
        else if (statement.equals("SELECT 1")) key="listenerHeartbeats";
        else key="otherStatements";
        sql.computeIfAbsent(key,k->new AtomicLong()).incrementAndGet();
    }
    static Connection counted(Connection real) {
        return (Connection)Proxy.newProxyInstance(WalletBenchmark.class.getClassLoader(),new Class<?>[]{Connection.class},(p,m,args)->{
            try {
                Object value=m.invoke(real,args);
                if (value instanceof Statement) {
                    Statement s=(Statement)value;
                    String prepared=args!=null && args.length>0 && args[0] instanceof String?(String)args[0]:null;
                    Class<?> type=value instanceof PreparedStatement?PreparedStatement.class:Statement.class;
                    return Proxy.newProxyInstance(WalletBenchmark.class.getClassLoader(),new Class<?>[]{type},(p2,m2,a2)->{
                        if (m2.getName().equals("execute")||m2.getName().equals("executeUpdate")||m2.getName().equals("executeQuery"))
                            count(prepared!=null?prepared:(String)a2[0]);
                        try{return m2.invoke(s,a2);}catch(InvocationTargetException e){throw e.getCause();}
                    });
                }
                return value;
            } catch(InvocationTargetException e){throw e.getCause();}
        });
    }
    static final class CountingSource implements DataSource {
        final HikariDataSource pool;
        CountingSource(HikariDataSource pool){this.pool=pool;}
        public Connection getConnection()throws SQLException{return counted(pool.getConnection());}
        public Connection getConnection(String a,String b)throws SQLException{return getConnection();}
        public PrintWriter getLogWriter(){return null;}public void setLogWriter(PrintWriter p){}public void setLoginTimeout(int n){}public int getLoginTimeout(){return 0;}
        public Logger getParentLogger(){return Logger.getGlobal();}public <T>T unwrap(Class<T> c)throws SQLException{throw new SQLException();}public boolean isWrapperFor(Class<?> c){return false;}
    }
    static final class Node implements AutoCloseable {
        final boolean modern;
        final HikariDataSource pool;
        final Object repo,wallet;
        final Method read,change,balance;
        final ThreadPoolExecutor writer,reader;
        Object sync;
        Node(boolean modern,String prefix,int index)throws Exception{
            this.modern=modern;
            HikariConfig c=new HikariConfig();
            c.setJdbcUrl(System.getenv("PLAYERPOINTS_TEST_PG_URL"));
            c.setDriverClassName("org.black_ixx.playerpoints.libs.postgresql.Driver");
            c.setUsername(System.getenv("PLAYERPOINTS_TEST_PG_USER"));c.setPassword(System.getenv("PLAYERPOINTS_TEST_PG_PASSWORD"));
            c.setMaximumPoolSize(3);c.setMinimumIdle(1);c.setConnectionTimeout(5000);
            c.addDataSourceProperty("options","-c statement_timeout=10000 -c lock_timeout=5000");
            c.addDataSourceProperty("ApplicationName","PP-wallet-ab-"+index);
            pool=new HikariDataSource(c);
            Class<?> r=Class.forName("org.black_ixx.playerpoints.storage.PgPointsRepository");
            repo=r.getConstructor(DataSource.class,String.class,int.class,boolean.class).newInstance(new CountingSource(pool),prefix,0,true);
            r.getMethod("initialize").invoke(repo);
            Class<?> w=Class.forName("org.black_ixx.playerpoints.storage.PgPointsWallet");
            wallet=w.getConstructor(r).newInstance(repo);
            read=w.getMethod("refresh",UUID.class);change=w.getMethod("change",UUID.class,PendingTransaction.class);balance=w.getMethod("balance",UUID.class);
            Field f=w.getDeclaredField(modern?"writer":"worker");f.setAccessible(true);writer=(ThreadPoolExecutor)f.get(wallet);
            if(modern){f=w.getDeclaredField("reader");f.setAccessible(true);reader=(ThreadPoolExecutor)f.get(wallet);}else reader=writer;
        }
        void startSync(int index)throws Exception{
            if(!modern)return;
            Class<?> manager=Class.forName("org.black_ixx.playerpoints.manager.PgPointsSyncManager");
            Class<?> factory=Class.forName("org.black_ixx.playerpoints.storage.PgPointsNotifications$ConnectionFactory");
            Object opener=Proxy.newProxyInstance(WalletBenchmark.class.getClassLoader(),new Class<?>[]{factory},(p,m,args)->{
                if(m.getName().equals("open")){
                    Properties properties=new Properties();properties.setProperty("user",System.getenv("PLAYERPOINTS_TEST_PG_USER"));properties.setProperty("password",System.getenv("PLAYERPOINTS_TEST_PG_PASSWORD"));
                    properties.setProperty("connectTimeout","5");properties.setProperty("socketTimeout","15");properties.setProperty("ApplicationName","PP-wallet-ab-listen-"+index);
                    return counted(new org.black_ixx.playerpoints.libs.postgresql.Driver().connect(System.getenv("PLAYERPOINTS_TEST_PG_URL"),properties));
                }
                throw new UnsupportedOperationException(m.getName());
            });
            String channel=(String)repo.getClass().getMethod("notificationChannel").invoke(repo);
            sync=manager.getConstructor(wallet.getClass(),factory,String.class,int.class,Consumer.class).newInstance(wallet,opener,channel,30,(Consumer<Throwable>)e->errors.add("sync:"+e));
            for(int i=index*500;i<(index+1)*500;i++)manager.getMethod("join",UUID.class).invoke(sync,IDS[i]);
        }
        CompletableFuture<Integer> refresh(UUID id)throws Exception{return (CompletableFuture<Integer>)read.invoke(wallet,id);}
        int value(int i){try{return (Integer)balance.invoke(wallet,IDS[i]);}catch(Exception e){throw new RuntimeException(e);}}
        void sweep(int index)throws Exception{
            if(modern){wallet.getClass().getMethod("reconcile",Collection.class).invoke(wallet,Arrays.asList(IDS).subList(index*500,(index+1)*500));return;}
            for(int i=index*500;i<(index+1)*500;i++){
                long start=System.nanoTime();refreshSubmitted.incrementAndGet();
                CompletableFuture<Integer> future=refresh(IDS[i]);pending.add(future);
                future.whenComplete((v,e)->{refreshLatency.add((System.nanoTime()-start)/1e6);if(e!=null)refreshErrors.incrementAndGet();});
            }
        }
        void write(int i,int amount,boolean measured,boolean observe)throws Exception{
            long start=System.nanoTime();
            if(measured){int n=outstanding.incrementAndGet();maxOutstanding.accumulateAndGet(n,Math::max);}
            CompletableFuture<Boolean> f=(CompletableFuture<Boolean>)change.invoke(wallet,IDS[i],new PendingTransaction(UpdateType.OFFSET,TransactionType.OFFSET,"Benchmark",null,amount));
            pending.add(f);
            f.whenComplete((ok,e)->{
                if(measured){outstanding.decrementAndGet();latency.add((System.nanoTime()-start)/1e6);}
                if(e!=null){if(measured)writeErrors.incrementAndGet();errors.add("write:"+e);}
                else if(Boolean.TRUE.equals(ok)){
                    int wanted=expected.addAndGet(i,amount);
                    if(measured){accepted.incrementAndGet();if(observe)waitingVisibility.add(new Visibility(i,wanted,System.nanoTime()));}
                }else if(measured)rejected.incrementAndGet();
            });
        }
        boolean idle(){return writer.getActiveCount()==0 && writer.getQueue().isEmpty() && reader.getActiveCount()==0 && reader.getQueue().isEmpty();}
        void stopSync()throws Exception{if(sync!=null){((AutoCloseable)sync).close();sync=null;}}
        public void close()throws Exception{stopSync();((AutoCloseable)wallet).close();pool.close();}
    }
    static final class Visibility {
        final int id,value;final long start;
        Visibility(int id,int value,long start){this.id=id;this.value=value;this.start=start;}
    }
    static void drain()throws Exception{
        long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(30);
        for(CompletableFuture<?> f:new ArrayList<>(pending))f.handle((v,e)->null).get(Math.max(1,deadline-System.nanoTime()),TimeUnit.NANOSECONDS);
        while(!nodes[0].idle()||!nodes[1].idle()){
            if(System.nanoTime()>deadline)throw new IllegalStateException("Wallet failed to drain");Thread.sleep(5);
        }
    }
    static String distribution(Collection<Double> input){
        List<Double> x=new ArrayList<>(input);Collections.sort(x);
        if(x.isEmpty())return "null";
        return String.format(Locale.ROOT,"{\"count\":%d,\"p50Ms\":%.6f,\"p95Ms\":%.6f,\"p99Ms\":%.6f,\"maxMs\":%.6f}",x.size(),x.get((int)Math.ceil(x.size()*.50)-1),x.get((int)Math.ceil(x.size()*.95)-1),x.get((int)Math.ceil(x.size()*.99)-1),x.get(x.size()-1));
    }
    public static void main(String[] args)throws Exception{
        boolean modern=args[0].equals("new");String scenario=args[1],prefix=args[2];int seconds=Integer.parseInt(args[3]);Path output=Paths.get(args[4]);
        for(int i=0;i<1000;i++)expected.set(i,10000);
        nodes=new Node[]{new Node(modern,prefix,0),new Node(modern,prefix,1)};
        try {
            try(Connection c=nodes[0].pool.getConnection();PreparedStatement s=c.prepareStatement("INSERT INTO "+prefix+"points(uuid,points) VALUES(?,10000)")){
                c.setAutoCommit(false);for(UUID id:IDS){s.setString(1,id.toString());s.addBatch();}s.executeBatch();c.commit();
            }
            for(int n=0;n<2;n++)for(int i=n*500;i<(n+1)*500;i++)nodes[n].refresh(IDS[i]).get();
            for(int n=0;n<2;n++)nodes[n].startSync(n);
            for(int n=0;n<2;n++)for(int i=0;i<100;i++){int id=(1-n)*500+i;nodes[n].write(id,1,false,false);nodes[n].write(id,-1,false,false);}
            drain();Thread.sleep(2500);drain();pending.clear();
            long initialLogs;
            try(Connection c=nodes[0].pool.getConnection();Statement s=c.createStatement();ResultSet r=s.executeQuery("SELECT COUNT(*) FROM "+prefix+"transaction_log")){r.next();initialLogs=r.getLong(1);}
            ScheduledExecutorService schedule=Executors.newScheduledThreadPool(3);
            final AtomicInteger ticks=new AtomicInteger();
            com.sun.management.OperatingSystemMXBean os=(com.sun.management.OperatingSystemMXBean)ManagementFactory.getOperatingSystemMXBean();
            long cpuStart=os.getProcessCpuTime();began=System.nanoTime();measuring.set(true);
            schedule.scheduleAtFixedRate(()->{
                int queue=nodes[0].writer.getQueue().size()+nodes[1].writer.getQueue().size();maxQueue.accumulateAndGet(queue,Math::max);
                maxConnections.accumulateAndGet(nodes[0].pool.getHikariPoolMXBean().getTotalConnections()+nodes[1].pool.getHikariPoolMXBean().getTotalConnections()+(modern?2:0),Math::max);
                for(Visibility v:waitingVisibility)if(nodes[v.id/500].value(v.id)==v.value && waitingVisibility.remove(v))visibilityLatency.add((System.nanoTime()-v.start)/1e6);
            },0,5,TimeUnit.MILLISECONDS);
            ScheduledFuture<?> sweeper = null, emitter = null;
            if(!modern && !scenario.equals("burst"))sweeper = schedule.scheduleAtFixedRate(()->{try{nodes[0].sweep(0);nodes[1].sweep(1);}catch(Exception e){errors.add("refresh:"+e);}},0,1,TimeUnit.SECONDS);
            if(scenario.equals("mixed"))emitter = schedule.scheduleAtFixedRate(()->{
                int tick=ticks.getAndIncrement();schedulerDelay.add(Math.max(0,(System.nanoTime()-began-tick*TimeUnit.MILLISECONDS.toNanos(20))/1e6));
                try{for(int n=0;n<2;n++)nodes[n].write((1-n)*500+(tick%500),(tick/500)%2==0?1:-1,true,true);}catch(Exception e){errors.add("emit:"+e);}
            },0,20,TimeUnit.MILLISECONDS);
            if(scenario.equals("burst")){
                for(int round=0;round<5;round++){
                    nodes[0].sweep(0);nodes[1].sweep(1);
                    for(int i=0;i<500;i++)for(int n=0;n<2;n++)nodes[n].write((1-n)*500+i,round%2==0?1:-1,true,false);
                    drain();pending.clear();Thread.sleep(500);
                }
            }else Thread.sleep(seconds*1000L);
            if(emitter!=null)emitter.cancel(false);
            drain();
            long visibilityDeadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(3);
            while(!waitingVisibility.isEmpty() && System.nanoTime()<visibilityDeadline)Thread.sleep(5);
            if(sweeper!=null)sweeper.cancel(false);
            schedule.shutdown();if(!schedule.awaitTermination(5,TimeUnit.SECONDS))throw new IllegalStateException("Emitter did not stop");
            drain();for(Node node:nodes)node.stopSync();drain();
            measuring.set(false);double elapsed=(System.nanoTime()-began)/1e9,cpu=(os.getProcessCpuTime()-cpuStart)/1e9;
            int mismatches=0;long sum=0,finalLogs;
            try(Connection c=nodes[0].pool.getConnection();Statement s=c.createStatement();ResultSet r=s.executeQuery("SELECT uuid,points FROM "+prefix+"points")){
                Map<UUID,Integer> actual=new HashMap<>();while(r.next())actual.put(UUID.fromString(r.getString(1)),r.getInt(2));
                for(int i=0;i<1000;i++){if(actual.get(IDS[i])!=expected.get(i))mismatches++;sum+=actual.get(IDS[i]);}
            }
            try(Connection c=nodes[0].pool.getConnection();Statement s=c.createStatement();ResultSet r=s.executeQuery("SELECT COUNT(*) FROM "+prefix+"transaction_log")){r.next();finalLogs=r.getLong(1)-initialLogs;}
            StringBuilder counts=new StringBuilder("{");for(String key:new TreeSet<>(sql.keySet())){if(counts.length()>1)counts.append(',');counts.append('"').append(key).append("\":").append(sql.get(key).get());}counts.append('}');
            boolean passed=mismatches==0&&finalLogs==accepted.get()&&errors.isEmpty()&&rejected.get()==0&&writeErrors.get()==0&&refreshErrors.get()==0&&waitingVisibility.isEmpty();
            String json=String.format(Locale.ROOT,"{\"version\":\"%s\",\"scenario\":\"%s\",\"secondsIncludingDrain\":%.6f,\"jvmCpuSeconds\":%.6f,\"accepted\":%d,\"rejected\":%d,\"writeErrors\":%d,\"refreshErrors\":%d,\"refreshTasksSubmitted\":%d,\"maxOutstandingTransactions\":%d,\"maxWalletQueue\":%d,\"maxPhysicalConnections\":%d,\"balanceMismatches\":%d,\"balanceTotal\":%d,\"transactionLogs\":%d,\"pendingVisibility\":%d,\"transactionLatency\":%s,\"visibilityLatency\":%s,\"refreshLatency\":%s,\"schedulerDelay\":%s,\"sql\":%s,\"passed\":%s,\"errorCount\":%d}",
                modern?"mulan.3":"mulan.2",scenario,elapsed,cpu,accepted.get(),rejected.get(),writeErrors.get(),refreshErrors.get(),refreshSubmitted.get(),maxOutstanding.get(),maxQueue.get(),maxConnections.get(),mismatches,sum,finalLogs,waitingVisibility.size(),distribution(latency),distribution(visibilityLatency),distribution(refreshLatency),distribution(schedulerDelay),counts,passed,errors.size());
            Files.write(output,json.getBytes(java.nio.charset.StandardCharsets.UTF_8));System.out.println(json);
            if(!passed)throw new IllegalStateException("Benchmark failed audit: "+errors);
        }finally{for(Node node:nodes)node.close();}
    }
}
