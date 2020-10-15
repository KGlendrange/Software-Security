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

import inf226.util.Pair;
import inf226.util.Triple;
import java.util.Arrays;

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
            final User notStoredUser = User.create(username);
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
        name = Encode.forHtml(name);
        try {
            Stored<Channel> channel
                = channelStore.save(new Channel(name,List.empty()));
            
            Maybe<Stored<Channel>> result = joinChannel(account, channel.identity);
            setRole(account,channel,account.value.user.value.name,"owner");
            return result;
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

            String[] allowed = new String[5];
            allowed[0] = "owner";
            allowed[1] = "moderator";
            allowed[2] = "participant";
            allowed[3] = "observer";
            if(!checkPermission(account, channel, null, allowed)){
                return Maybe.nothing();
            }      


            //Default role when joining
            String role = "Participant";
            String alias = channel.value.name;
            
            Util.updateSingle(account,
                              accountStore,
                              a -> a.value.joinChannel(alias,role,channel));
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
        String[] allowed = new String[5];
        allowed[0] = "owner";
        allowed[1] = "moderator";
        allowed[3] = "participant";
        if(!checkPermission(account, channel, null, allowed)){
            return Maybe.nothing();
        }
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

    public boolean checkPermission(Stored<Account> account, Stored<Channel> channel, Stored<Channel.Event> event, String[] allowed){
        if(event!= null){
            if(event.value.sender.equals(account.value.user.value.name)) return true;
        
            //Also check if account.value.channels which are made up of a triple (alias,role,channel) has an alias same as the channel
            //as well as if allowed.contains role
            account.value.channels.forEach(e -> {
                //match to channel
                if(e.first.equals(channel.value.name)){
                    //if your role in this channel is any of the ones in the allowed array
                    if(Arrays.asList(allowed).contains(e.second)){
                        final boolean flag = true;
                    }
                }
            });
        }
       

        System.err.println("permission ALWAYS allowed for now (needs fix)");
        return true;
    }
    
    public Stored<Channel> deleteEvent(Stored<Account> account, Stored<Channel> channel, Stored<Channel.Event> event) {
        String[] allowed = new String[5];
        allowed[0] = "owner";
        allowed[1] = "moderator";
        if(!checkPermission(account, channel, event, allowed)){
            return channel;
        }
        try {
            Util.deleteSingle(event , channelStore.eventStore);
            return channelStore.noChangeUpdate(channel.identity);
        } catch (SQLException er) {
            System.err.println("While deleting event " + event.identity +":\n" + er);
        } catch (DeletedException er) {
        }
        return channel;
    }
    
    public Stored<Channel> editMessage(Stored<Account> account, Stored<Channel> channel,
                                       Stored<Channel.Event> event,
                                       String newMessage) {
        String[] allowed = new String[5];
        allowed[0] = "owner";
        allowed[1] = "moderator";
        if(!checkPermission(account, channel, event, allowed)){
            return channel;
        }
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

    public void setRole(Stored<Account> activaterAccount, Stored<Channel> channel, String user, String new_role){
        String[] allowed = new String[5];
        allowed[0] = "owner";
        
        if(!checkPermission(activaterAccount,channel, null, allowed)){
            return;
        }


        try{
            // Get all the channels associated with this account
            final List.Builder<Triple<String,String,Stored<Channel>>> channels = List.builder();
            Stored<Account> account = accountStore.lookup(user);
            System.err.println("In channel: "+channel.value.name+" setting role: "+new_role+" to user: "+user);


            account.value.channels.forEach(e -> {
                final String alias = e.first;
                String role = e.second;
                final Stored<Channel> ch = e.third;
                if(alias.equals(channel.value.name)){
                    System.err.println("changing role from : "+role+" to "+new_role);
                    role = new_role;
                }

                channels.accept(
                    new Triple<String,String,Stored<Channel>>(
                        alias,role,ch));
        
            });
            //Finished
            System.err.println("channelsTest: ");
            List<Triple<String,String,Stored<Channel>>> chs = channels.getList();
            chs.forEach(e ->{
                System.err.println(e.first+"-"+e.second+"-"+e.third);
            });

            //make a new account that is a clone of the previous account, except one entry in channels has changed the role
            Account new_account = new Account(account.value.user,chs,account.value.hashed);
            Stored<Account> result = accountStore.update(account,new_account);
            System.err.println("result: ");
            result.value.channels.forEach(e->{
                System.err.println(e.first+"-"+e.second+"-"+e.third);
            });
            

        }catch(Exception e){

        }
        

        
    }

   
}


