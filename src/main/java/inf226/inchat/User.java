package inf226.inchat;
import java.time.Instant;

/**
 * The User class holds the public information
 * about a user.
 **/
public final class User {
    public final Instant joined;

    public final String name;

    public User(String name,Instant joined){
        this.joined = joined;
        this.name = name;

    }
    
    public static User create(String name) {
        return new User(new UserName(name).getUserName(), Instant.now());
    }


}

