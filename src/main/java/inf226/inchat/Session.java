package inf226.inchat;
import java.time.Instant;
import inf226.storage.*;



public final class Session {
    final Stored<Account> account;
    final Instant expiry;

    public Session( Stored<Account> account, Instant expiry) {
        this.account = account;
        this.expiry = expiry;
    }
}
