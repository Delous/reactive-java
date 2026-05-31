package me.delous.reactive.subscription;

import me.delous.reactive.api.Disposable;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

public class Subscription implements Disposable {
    private static final Logger LOGGER = Logger.getLogger(Subscription.class.getName());

    private final AtomicBoolean cancelled = new AtomicBoolean(false);
    private final List<Disposable> cancelActions = new CopyOnWriteArrayList<>();

    public void addCancelAction(Disposable disposable) {
        if (isCancelled()) {
            disposable.dispose();
            return;
        }
        cancelActions.add(disposable);
        if (isCancelled() && cancelActions.remove(disposable)) {
            disposable.dispose();
        }
    }

    @Override
    public void dispose() {
        if (cancelled.compareAndSet(false, true)) {
            LOGGER.info("event=subscription.cancelled");
            for (Disposable cancelAction : cancelActions) {
                cancelAction.dispose();
            }
            cancelActions.clear();
        }
    }

    @Override
    public boolean isDisposed() {
        return isCancelled();
    }

    public boolean isCancelled() {
        return cancelled.get();
    }
}
