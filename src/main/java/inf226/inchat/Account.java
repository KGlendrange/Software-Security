package inf226.inchat;
import inf226.util.immutable.List;
import inf226.util.Pair;


import inf226.storage.*;

import com.lambdaworks.crypto.SCryptUtil;


/**
 * The Account class holds all information private to
 * a specific user.
 **/
public final class Account {
    public final Stored<User> user;
    public final List<Pair<String,Stored<Channel>>> channels;
    
    public Account(Stored<User> user, 
                   List<Pair<String,Stored<Channel>>> channels) {
        this.user = user;
        this.channels = channels;

    }
    
    /**
     * Create a new Account.
     **/
    public static Account create(Stored<User> user,
                                 String password) {
        return new Account(user,List.empty());
    }
    
    
    public Account joinChannel(String alias, Stored<Channel> channel) {
        Pair<String,Stored<Channel>> entry
            = new Pair<String,Stored<Channel>>(alias,channel);
        return new Account
                (user,
                 List.cons(entry,
                           channels));
    }


    public boolean checkPassword(String password) {
        //SCrypt on the given password vs the hashed password
        return SCryptUtil.check(password, this.user.value.password);
    }
    
    
}
