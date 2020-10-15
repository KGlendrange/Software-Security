package inf226.util;
import inf226.util.immutable.List;
import java.lang.Throwable;
import java.util.function.Function;

import inf226.storage.*;


public class Util {
   public static<E extends Throwable> void throwMaybe(Maybe<E> exception) throws E {
       try { throw exception.get(); }
       catch (Maybe.NothingException e) { /* Intensionally left blank */ }
   }
   
    
    public static<A,B,C> Maybe<C> lookup(List<Triple<A,B,C>> list, A key){
        final Maybe.Builder<C> result
            = new Maybe.Builder<C>();
        list.forEach(triple -> {
            if(triple.first.equals(key))
                result.accept(triple.third);
        });
        return result.getMaybe();
    }

   
    public static<A,Q, E extends Exception>
        Stored<A> updateSingle(Stored<A> stored,
                                Storage<A,E> storage,
                                Function<Stored<A>,A> update)
            throws E, DeletedException {
        boolean updated = true;
        while(true) {
            try {
                return storage.update(stored,update.apply(stored));
            } catch (UpdatedException e) {
                stored = (Stored<A>)e.newObject;
            }
        }
    }
    
    public static<A,Q, E extends Exception> void deleteSingle(Stored<A> stored, Storage<A,E> storage)
        throws E {
        while(true) {
            try {
                storage.delete(stored);
            } catch (UpdatedException e) {
                stored = (Stored<A>)e.newObject;
            } catch (DeletedException e) {
                return;
            }
        }
    }
}

