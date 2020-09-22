package inf226.inchat;

import inf226.util.immutable.List;
import inf226.storage.Stored;
import java.time.Instant;

public final class Channel {
    public final String name;
    public final List<Stored<Event>> events;
    
    public Channel(String name, List<Stored<Event>> events) {
        this.name=name;
        this.events=events;
    }
    
    public Channel postEvent(Stored<Event> event) {
        return new Channel(name, List.cons(event,events));
    }
    
    public static class Event {
        public static enum Type {
            message(0),join(1);
            public final Integer code;
            Type(Integer code){this.code=code;}
            public static Type fromInteger(Integer i) {
                if (i.equals(0))
                    return message;
                else if (i.equals(1))
                    return join;
                else
                    throw new IllegalArgumentException("Invalid Channel.Event.Type code:" + i);
            }
        };
        public final Type type;
        public final Instant time;
        public final String sender;
        public final String message;
        
        /**
         * Copy constructor
         **/
        public Event(Instant time, String sender, Type type, String message) {
            if (time == null) {
                throw new IllegalArgumentException("Event time cannot be null");
            }
            if (type.equals(message) && message == null) {
                throw new IllegalArgumentException("null in Event creation");
            }
            this.time=time;
            this.sender=sender;
            this.type=type;
            this.message=message;
        }
        public static Event createMessageEvent(Instant time, String sender, String message) {
            return new Event(time,
                             sender,
                             Type.message,
                             message);
        }
        public static Event createJoinEvent(Instant time, String user) {
            return new Event(time,
                             user,
                             Type.join,
                             null);
        }
        
        public Event setMessage(String message) {
            return new Event(time, sender , type , message);
        }
    }
}
