package inf226.inchat;

import inf226.storage.*;
import inf226.util.Maybe;
import inf226.util.Util;

import java.util.TreeMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.UUID;
import java.time.Instant;
import java.sql.SQLException;


import inf226.util.immutable.List;

import org.owasp.encoder.Encode;

/**
 * This class models the chat logic.
 *
 * It provides an abstract interface to
 * usual chat server actions.
 *
 **/

public class InChat {
    private final UserStorage userStore;
    private final ChannelStorage channelStore;
    private final AccountStorage accountStore;
    private final SessionStorage sessionStore;
    private final Map<UUID,List<Consumer<Channel.Event>>> eventCallbacks
        = new TreeMap<UUID,List<Consumer<Channel.Event>>>();

    public InChat(UserStorage userStore,
                  ChannelStorage channelStore,
                  AccountStorage accountStore,
                  SessionStorage sessionStore) {
        this.userStore=userStore;
        this.channelStore=channelStore;
        this.accountStore=accountStore;
        this.sessionStore=sessionStore;
    }


    /**
     * Log in a user to the chat.
     */
    public Maybe<Stored<Session>> login(String username, String password) {
        // Here you can implement login.
        try {
            final Stored<Account> account = accountStore.lookup(username);
            System.err.println("trying to log in on account: "+ account.value);
            //Checking password
            
            if(account.value.checkPassword(password)){
                final Stored<Session> session =
                sessionStore.save(new Session(account, Instant.now().plusSeconds(60*60*24)));
                return Maybe.just(session); 
            }else{
                System.err.println("Password was not correct");
            }
        } catch (SQLException e) {
        } catch (DeletedException e) {
        }
        return Maybe.nothing();
    }
    
    /**
     * Register a new user.
     */
    public Maybe<Stored<Session>> register(String username, String password) {
        try {
            //User now stores the SCrypt hash of their password
            System.err.println("Trying to register user: \"" + username
            + "\" with password \"" + password + "\" inside inchat register()");
            final User notStoredUser = User.create(username,password);
            System.err.println("Made the user : "+ notStoredUser.toString());
            final Stored<User> user =
                userStore.save(notStoredUser);
            System.err.println("Did register a user!!: \"" + username
                + "\" with password \"" + password + "\" inside inchat register()");
            final Stored<Account> account =
                accountStore.save(Account.create(user, password));
            final Stored<Session> session =
                sessionStore.save(new Session(account, Instant.now().plusSeconds(60*60*24)));
            return Maybe.just(session); 
        } catch (SQLException e) {
            return Maybe.nothing();
        }
    }
    
    /**
     * Restore a previous session.
     */
    public Maybe<Stored<Session>> restoreSession(UUID sessionId) {
        try {
            return Maybe.just(sessionStore.get(sessionId));
        } catch (SQLException e) {
            System.err.println("When restoring session:" + e);
            return Maybe.nothing();
        } catch (DeletedException e) {
            return Maybe.nothing();
        }
    }
    
    /**
     * Log out and invalidate the session.
     */
    public void logout(Stored<Session> session) {
        try {
            Util.deleteSingle(session,sessionStore);
        } catch (SQLException e) {
            System.err.println("When loging out of session:" + e);
        }
    }
    
    /**
     * Create a new channel.
     */
    public Maybe<Stored<Channel>> createChannel(Stored<Account> account,
                                                String name) {
        try {
            Stored<Channel> channel
                = channelStore.save(new Channel(name,List.empty()));
            return joinChannel(account, channel.identity);
        } catch (SQLException e) {
            System.err.println("When trying to create channel " + name +":\n" + e);
        }
        return Maybe.nothing();
    }
    
    /**
     * Join a channel.
     */
    public Maybe<Stored<Channel>> joinChannel(Stored<Account> account,
                                              UUID channelID) {
        try {
            Stored<Channel> channel = channelStore.get(channelID);
            Util.updateSingle(account,
                              accountStore,
                              a -> a.value.joinChannel(channel.value.name,channel));
            Stored<Channel.Event> joinEvent
                = channelStore.eventStore.save(
                    Channel.Event.createJoinEvent(Instant.now(),
                        account.value.user.value.name));
            return Maybe.just(
                Util.updateSingle(channel,
                                  channelStore,
                                  c -> c.value.postEvent(joinEvent)));
        } catch (DeletedException e) {
            // This channel has been deleted.
        } catch (SQLException e) {
            System.err.println("When trying to join " + channelID +":\n" + e);
        }
        return Maybe.nothing();
    }
    
    /**
     * Post a message to a channel.
     */
    public Maybe<Stored<Channel>> postMessage(Stored<Account> account,
                                              Stored<Channel> channel,
                                              String message) {
        
        System.err.println("Trying to post a message in inChat postMessage()");
        try {
            Stored<Channel.Event> event
                = channelStore.eventStore.save(
                    Channel.Event.createMessageEvent(Instant.now(),
                        account.value.user.value.name, message));
            try {
                return Maybe.just(
                    Util.updateSingle(channel,
                                      channelStore,
                                      c -> c.value.postEvent(event)));
            } catch (DeletedException e) {
                // Channel was already deleted.
                // Let us pretend this never happened
                Util.deleteSingle(event, channelStore.eventStore);
            }
        } catch (SQLException e) {
            System.err.println("When trying to post message in " + channel.identity +":\n" + e);
        }
        return Maybe.nothing();
    }
    
    /**
     * A blocking call which returns the next state of the channel.
     */
    public Maybe<Stored<Channel>> waitNextChannelVersion(UUID identity, UUID version) {
        try {
            return Maybe.just(channelStore.waitNextVersion(identity, version));
        } catch (SQLException e) {
            System.err.println("While waiting for the next message in " + identity +":\n" + e);
        } catch (DeletedException e) {
            // Channel deleted.
        }
        return Maybe.nothing();
    }
    
    public Maybe<Stored<Channel.Event>> getEvent(UUID eventID) {
        try {
            return Maybe.just(channelStore.eventStore.get(eventID));
        } catch (SQLException e) {
            return Maybe.nothing();
        } catch (DeletedException e) {
            return Maybe.nothing();
        }
    }
    
    public Stored<Channel> deleteEvent(Stored<Channel> channel, Stored<Channel.Event> event) {
        try {
            Util.deleteSingle(event , channelStore.eventStore);
            return channelStore.noChangeUpdate(channel.identity);
        } catch (SQLException er) {
            System.err.println("While deleting event " + event.identity +":\n" + er);
        } catch (DeletedException er) {
        }
        return channel;
    }
    
    public Stored<Channel> editMessage(Stored<Channel> channel,
                                       Stored<Channel.Event> event,
                                       String newMessage) {
        try{
            Util.updateSingle(event,
                            channelStore.eventStore,
                            e -> e.value.setMessage(newMessage));
            return channelStore.noChangeUpdate(channel.identity);
        } catch (SQLException er) {
            System.err.println("While deleting event " + event.identity +":\n" + er);
        } catch (DeletedException er) {
        }
        return channel;
    }
}


