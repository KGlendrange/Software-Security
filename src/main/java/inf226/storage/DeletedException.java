package inf226.storage;

public class DeletedException extends Exception {
    private static final long serialVersionUID = 416363032598879968L;
    public DeletedException() {
        super("Object was deleted");
    }

     @Override
     public Throwable fillInStackTrace() {
         return this;
     }
}
