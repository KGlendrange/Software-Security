package inf226.inchat;
import inf226.util.immutable.List;
import inf226.util.Triple;


import inf226.storage.*;

import com.lambdaworks.crypto.SCryptUtil;


/**
 * The Account class holds all information private to
 * a specific user.
 **/
public final class Account {
    public final Stored<User> user;
    public final List<Triple<String,String,Stored<Channel>>> channels;
    public final Password hashed; 
    
    public Account(Stored<User> user, 
                   List<Triple<String,String,Stored<Channel>>> channels, Password hashed) {
        this.user = user;
        this.channels = channels;
        this.hashed = hashed;

    }
    
    /**
     * Create a new Account.
     **/
    public static Account create(Stored<User> user,
                                 String password) {
        return new Account(user,List.empty(),Password.create(password));
    }
    
    
    public Account joinChannel(String alias, String role, Stored<Channel> channel) {
        Triple<String,String,Stored<Channel>> entry
            = new Triple<String,String,Stored<Channel>>(alias,role,channel);
        return new Account
                (user,
                 List.cons(entry,channels),
                 hashed);
    }


    public boolean checkPassword(String password) {
        //SCrypt on the given password vs the hashed password
        boolean result = SCryptUtil.check(password, hashed.toString());
        return result;
    }
    
    
}
