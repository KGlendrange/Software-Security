package inf226.inchat;

import com.lambdaworks.crypto.SCryptUtil;


/**
 * The Password class holds the public information
 * about a Password.
 **/
public final class Password {
    public final String hashed;

    public Password(String hashed) {
        this.hashed = hashed;
    }
    
    public static Password create(String password) {
        if(password.length() < 8){
            throw new IllegalArgumentException();
        }
        //Scrypt on the password, with N = 16384, r = 8, p = 1
        String hashed = SCryptUtil.scrypt(password,16384,8,1);
        return new Password(hashed);
    }
    public String toString(){
        return this.hashed;
    }
}


