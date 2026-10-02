package org.black_ixx.playerpoints.models;
import org.junit.Test;
import static org.junit.Assert.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
public class PointWriteQueueTest {
 private PendingTransaction offset(int n){return new PendingTransaction(UpdateType.OFFSET,TransactionType.OFFSET,"test",null,n);}
 @Test public void acceptedWhileBatchInFlightIsNeverLost(){
  PointWriteQueue q=new PointWriteQueue();UUID id=UUID.randomUUID();
  assertTrue(q.add(id,offset(-10),()->100));Map<UUID,List<PendingTransaction>> first=q.drain();
  assertEquals(90,q.balance(id,()->100));assertTrue(q.add(id,offset(-10),()->100));
  assertEquals(1,first.get(id).size());q.committed(id,90,v->{});assertEquals(80,q.balance(id,()->90));
  Map<UUID,List<PendingTransaction>> second=q.drain();assertEquals(-10,second.get(id).get(0).getAmount());q.committed(id,80,v->{});assertEquals(80,q.balance(id,()->80));
 }
 @Test public void concurrentDebitAndPersistenceNeverOverspendOrLoseOperations()throws Exception{
  PointWriteQueue q=new PointWriteQueue();UUID id=UUID.randomUUID();AtomicInteger stored=new AtomicInteger(1000),accepted=new AtomicInteger();AtomicBoolean producing=new AtomicBoolean(true);ExecutorService pool=Executors.newFixedThreadPool(9);CountDownLatch go=new CountDownLatch(1);
  Future<?> writer=pool.submit(()->{try{go.await();while(producing.get()){persist(q,id,stored);Thread.yield();}persist(q,id,stored);}catch(InterruptedException e){Thread.currentThread().interrupt();throw new IllegalStateException(e);}});
  List<Future<?>> producers=new ArrayList<>();for(int i=0;i<8;i++)producers.add(pool.submit(()->{try{go.await();for(int j=0;j<250;j++)if(q.add(id,offset(-1),stored::get))accepted.incrementAndGet();}catch(InterruptedException e){throw new IllegalStateException(e);}}));go.countDown();for(Future<?> f:producers)f.get(10,TimeUnit.SECONDS);producing.set(false);writer.get(10,TimeUnit.SECONDS);pool.shutdownNow();assertEquals(1000,accepted.get());assertEquals(0,stored.get());assertEquals(0,q.balance(id,stored::get));
 }
 private void persist(PointWriteQueue q,UUID id,AtomicInteger stored){Map<UUID,List<PendingTransaction>> b=q.drain();if(!b.containsKey(id))return;for(PendingTransaction t:b.get(id))stored.addAndGet(t.getAmount());q.committed(id,stored.get(),stored::set);}
 @Test public void staleRefreshCannotOverwriteLocalAcceptedChanges(){PointWriteQueue q=new PointWriteQueue();UUID id=UUID.randomUUID();long before=q.generation(id);AtomicInteger cache=new AtomicInteger(100);q.add(id,offset(-10),cache::get);q.drain();q.committed(id,90,cache::set);q.publish(id,before,100,cache::set);assertEquals(90,cache.get());}
 @Test public void unknownPersistenceFailureRejectsFurtherWrites(){PointWriteQueue q=new PointWriteQueue();q.failed();assertThrows(IllegalStateException.class,()->q.add(UUID.randomUUID(),offset(1),()->0));assertThrows(IllegalStateException.class,q::drain);}
}
