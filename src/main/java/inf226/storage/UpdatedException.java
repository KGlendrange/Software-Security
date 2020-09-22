package inf226.storage;
import inf226.storage.Stored;

public class UpdatedException extends Exception {
    private static final long serialVersionUID = 8516366302597379968L;
    public final Stored newObject;
    public UpdatedException(Stored newObject) {
        super("Object was updated");
        this.newObject = newObject;
    }

     @Override
     public Throwable fillInStackTrace() {
         return this; // We do not want stack traces for these exceptions.
     }
}
