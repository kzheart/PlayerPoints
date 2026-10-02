package org.black_ixx.playerpoints.models;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.IntConsumer;
import java.util.function.IntSupplier;

/** Keeps accepted balances visible while immutable batches are being persisted. */
public final class PointWriteQueue {
    private static final class Account {
        int value;
        final List<PendingTransaction> pending = new ArrayList<>();
        Account(int value) { this.value = value; }
    }
    private final Map<UUID, Account> accounts = new HashMap<>();
    private final Map<UUID, Long> generations = new HashMap<>();
    private boolean healthy = true;

    public synchronized int balance(UUID id, IntSupplier base) {
        Account account = accounts.get(id);
        return account == null ? base.getAsInt() : account.value;
    }
    public synchronized boolean add(UUID id, PendingTransaction transaction, IntSupplier base) {
        if (!healthy) throw new IllegalStateException("Points persistence failed; reconciliation required");
        Account account = accounts.get(id);
        int current = account == null ? base.getAsInt() : account.value;
        // A cold database read can initialize the starting-balance transaction reentrantly.
        account = accounts.get(id);
        if (account != null) current = account.value;
        long value = transaction.getUpdateType() == UpdateType.SET ? transaction.getAmount() : (long) current + transaction.getAmount();
        if (value < 0 || value > Integer.MAX_VALUE) return false;
        if (account == null) { account = new Account(current); accounts.put(id, account); }
        account.value = (int) value;
        account.pending.add(transaction);
        generations.put(id, generation(id) + 1);
        return true;
    }
    /** Caller serializes writers; batches never contain a deque producers can still mutate. */
    public synchronized Map<UUID, List<PendingTransaction>> drain() {
        if (!healthy) throw new IllegalStateException("Points persistence failed");
        Map<UUID, List<PendingTransaction>> result = new HashMap<>();
        accounts.forEach((id, account) -> {
            if (!account.pending.isEmpty()) {
                result.put(id, new ArrayList<>(account.pending));
                account.pending.clear();
            }
        });
        return result;
    }
    public synchronized void committed(UUID id, int persisted, IntConsumer cache) {
        Account account = accounts.get(id);
        cache.accept(persisted);
        if (account != null && account.pending.isEmpty()) accounts.remove(id);
    }
    public synchronized void failed() { healthy = false; }
    public synchronized long generation(UUID id) { return generations.getOrDefault(id, 0L); }
    public synchronized void publish(UUID id, long expected, int value, IntConsumer cache) {
        if (!accounts.containsKey(id) && generation(id) == expected) cache.accept(value);
    }
    public synchronized void remove(UUID id) { accounts.remove(id); generations.put(id, generation(id) + 1); }
    public synchronized void clear() { accounts.clear(); generations.replaceAll((id, generation) -> generation + 1); }
}
