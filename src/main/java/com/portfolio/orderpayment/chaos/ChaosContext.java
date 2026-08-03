package com.portfolio.orderpayment.chaos;

/**
 * Explicit fault injection for the saga, scoped to the current request thread. The demo UI and the
 * recovery tests drive the failure paths through the same API surface as real traffic — an
 * {@code X-Chaos} header parsed at the controller (gated by {@code chaos.enabled}) — instead of
 * hidden test hooks, so every advertised failure mode is reproducible with one curl:
 *
 * <ul>
 *   <li>{@code payment-transient:N} — the PSP throws (timeout-style) for the first N attempts;</li>
 *   <li>{@code payment-down} — every PSP attempt throws (exhausts the retry budget);</li>
 *   <li>{@code confirm-crash} — the confirm step fails after a successful authorization, forcing
 *       the void + release compensation pair;</li>
 *   <li>{@code poison-event} — the outbox payload is corrupted, sending the consumer to the DLT.</li>
 * </ul>
 *
 * <p>The saga runs synchronously on the request thread, which is what makes a ThreadLocal the
 * right scope; {@code close()} in the controller's finally block prevents leakage across pooled
 * threads.
 */
public final class ChaosContext {

    private static final ThreadLocal<State> CURRENT = new ThreadLocal<>();

    private static final class State {
        int transientPaymentFailures;
        boolean paymentDown;
        boolean confirmCrash;
        boolean poisonEvent;
    }

    private ChaosContext() {
    }

    public static void open(String directives) {
        State state = new State();
        for (String raw : directives.split(",")) {
            String directive = raw.trim().toLowerCase();
            if (directive.startsWith("payment-transient")) {
                int colon = directive.indexOf(':');
                state.transientPaymentFailures =
                        colon < 0 ? 2 : Integer.parseInt(directive.substring(colon + 1));
            } else if (directive.equals("payment-down")) {
                state.paymentDown = true;
            } else if (directive.equals("confirm-crash")) {
                state.confirmCrash = true;
            } else if (directive.equals("poison-event")) {
                state.poisonEvent = true;
            }
        }
        CURRENT.set(state);
    }

    public static void close() {
        CURRENT.remove();
    }

    /** One injected PSP failure consumed per call — {@code payment-down} never runs out. */
    public static boolean consumePaymentFailure() {
        State state = CURRENT.get();
        if (state == null) {
            return false;
        }
        if (state.paymentDown) {
            return true;
        }
        if (state.transientPaymentFailures > 0) {
            state.transientPaymentFailures--;
            return true;
        }
        return false;
    }

    public static boolean consumeConfirmCrash() {
        State state = CURRENT.get();
        if (state != null && state.confirmCrash) {
            state.confirmCrash = false;
            return true;
        }
        return false;
    }

    public static boolean poisonEvent() {
        State state = CURRENT.get();
        return state != null && state.poisonEvent;
    }
}
