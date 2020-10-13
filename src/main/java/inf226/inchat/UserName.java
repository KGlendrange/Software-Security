package inf226.inchat;

/**
 * The User class holds the public information
 * about a username.
 **/
     public final class UserName {
    public final String username;

    public UserName(String username) {
        this.username = username;
    }
    
    public static UserName create(String username) {
        return new UserName(username);
    }
    public String getUserName(){
        return this.username;
    }
}

