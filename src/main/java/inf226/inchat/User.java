package inf226.inchat;
import java.time.Instant;

/**
 * The User class holds the public information
 * about a user.
 **/
public final class User {
    public final Instant joined;

    public final String name;
    public final String password;
    

    public User(String name, String password,Instant joined){
        this.joined = joined;
        this.password = password;
        this.name = new UserName(name).getUserName();

    }
    
    public static User create(String name, String password) {
        return new User(name, new Password(password).getPassword(), Instant.now());
    }


}

