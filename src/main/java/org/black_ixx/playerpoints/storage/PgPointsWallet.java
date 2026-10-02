package org.black_ixx.playerpoints.storage;

import java.util.*;
import java.util.concurrent.*;
import java.util.function.Consumer;
import org.black_ixx.playerpoints.models.PendingTransaction;

/** Bounded owned worker; committed revisions keep stale refreshes from overwriting a newer debit. */
public final class PgPointsWallet implements AutoCloseable {
    private final PgPointsRepository repository;
    private final ThreadPoolExecutor worker;
    private final ConcurrentMap<UUID,PgPointsRepository.Balance> cache=com.google.common.cache.CacheBuilder.newBuilder().maximumSize(10000).expireAfterAccess(5,TimeUnit.MINUTES).<UUID,PgPointsRepository.Balance>build().asMap();
    public PgPointsWallet(PgPointsRepository repository){
        this.repository=repository;
        this.worker=new ThreadPoolExecutor(1,1,0,TimeUnit.SECONDS,new ArrayBlockingQueue<>(10000),r->{Thread t=new Thread(r,"PlayerPoints-PG");t.setDaemon(true);return t;},new ThreadPoolExecutor.AbortPolicy());
    }
    private <T> CompletableFuture<T> submit(Callable<T> operation){
        CompletableFuture<T> future=new CompletableFuture<>();
        try{worker.execute(()->{try{future.complete(operation.call());}catch(Throwable e){future.completeExceptionally(e);}});}
        catch(RejectedExecutionException e){future.completeExceptionally(e);}
        return future;
    }
    private void publish(UUID id,PgPointsRepository.Balance b){cache.compute(id,(k,old)->old==null||b.revision>=old.revision?b:old);}
    public CompletableFuture<Integer> refresh(UUID id){return submit(()->{PgPointsRepository.Balance b=repository.read(id);publish(id,b);return b.points;});}
    public int balance(UUID id){PgPointsRepository.Balance b=cache.get(id);return b==null?await(refresh(id)):b.points;}
    public CompletableFuture<Boolean> change(UUID id,PendingTransaction t){return submit(()->{PgPointsRepository.Balance b=repository.change(id,t);if(b==null)return false;publish(id,b);return true;});}
    public CompletableFuture<Boolean> transfer(UUID a,UUID b,PendingTransaction debit,PendingTransaction credit){return submit(()->{Map<UUID,PgPointsRepository.Balance> values=repository.transfer(a,b,debit,credit);if(values==null)return false;values.forEach(this::publish);return true;});}
    public void refreshCached(Consumer<Throwable> error){for(UUID id:new ArrayList<>(cache.keySet()))refresh(id).whenComplete((v,e)->{if(e!=null)error.accept(e);});}
    public CompletableFuture<Void> offsetAll(int amount){return submit(()->{repository.offsetAll(amount).forEach(this::publish);return null;});}
    public void clear(){cache.clear();}
    public static <T> T await(CompletableFuture<T> future){
        try{return future.get(30,TimeUnit.SECONDS);}
        catch(InterruptedException e){Thread.currentThread().interrupt();throw new IllegalStateException("Points operation interrupted; outcome unknown",e);}
        catch(ExecutionException|TimeoutException e){throw new IllegalStateException("Points operation failed; reconcile before retry",e);}
    }
    @Override public void close(){worker.shutdown();try{if(!worker.awaitTermination(30,TimeUnit.SECONDS))throw new IllegalStateException("PlayerPoints PG shutdown still has pending operations");}catch(InterruptedException e){Thread.currentThread().interrupt();throw new IllegalStateException(e);}}
}
