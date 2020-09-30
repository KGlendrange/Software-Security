package inf226.inchat;

import com.lambdaworks.crypto.SCryptUtil;


/**
 * The Password class holds the public information
 * about a Password.
 **/
public final class Password {
    public final String hashed;

    public Password(String password) {
        //Scrypt on the password, with N = 16384, r = 8, p = 1
        this.hashed = SCryptUtil.scrypt(password,16384,8,1);
    }
    
    public static Password create(String password) {
        return new Password(password);
    }
    public String getPassword(){
        return this.hashed;
    }
}


