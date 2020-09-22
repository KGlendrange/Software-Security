package inf226.inchat;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.UUID;
import java.util.TreeMap;
import java.util.Map;
import java.util.function.Consumer;

import inf226.storage.*;

import inf226.util.immutable.List;
import inf226.util.*;

public final class ChannelStorage
    implements Storage<Channel,SQLException> {
    
    final Connection connection;
    private Map<UUID,List<Consumer<Stored<Channel>>>> waiters
        = new TreeMap<UUID,List<Consumer<Stored<Channel>>>>();
    public final EventStorage eventStore;
    
    public ChannelStorage(Connection connection,
                          EventStorage eventStore) 
      throws SQLException {
        this.connection = connection;
        this.eventStore = eventStore;
        
        connection.createStatement()
                .executeUpdate("CREATE TABLE IF NOT EXISTS Channel (id TEXT PRIMARY KEY, version TEXT, name TEXT)");
        connection.createStatement()
                .executeUpdate("CREATE TABLE IF NOT EXISTS ChannelEvent (channel TEXT, event TEXT, ordinal INTEGER, PRIMARY KEY(channel,event), FOREIGN KEY(channel) REFERENCES Channel(id) ON DELETE CASCADE, FOREIGN KEY(event) REFERENCES Event(id) ON DELETE CASCADE)");
    }
    
    @Override
    public Stored<Channel> save(Channel channel)
      throws SQLException {
        
        final Stored<Channel> stored = new Stored<Channel>(channel);
        String sql =  "INSERT INTO Channel VALUES('" + stored.identity + "','"
                                                  + stored.version  + "','"
                                                  + channel.name  + "')";
        connection.createStatement().executeUpdate(sql);
        
        // Write the list of events
        final Maybe.Builder<SQLException> exception = Maybe.builder();
        final Mutable<Integer> ordinal = new Mutable<Integer>(0);
        channel.events.forEach(event -> {
            final String msql = "INSERT INTO ChannelEvent VALUES('" + stored.identity + "','"
                                                                        + event.identity + "','"
                                                                        + ordinal.get().toString() + "')";
            try { connection.createStatement().executeUpdate(msql); }
            catch (SQLException e) { exception.accept(e) ; }
            ordinal.accept(ordinal.get() + 1);
        });

        Util.throwMaybe(exception.getMaybe());
        return stored;
    }
    
    @Override
    public synchronized Stored<Channel> update(Stored<Channel> channel,
                                            Channel new_channel)
        throws UpdatedException,
            DeletedException,
            SQLException {
        final Stored<Channel> current = get(channel.identity);
        final Stored<Channel> updated = current.newVersion(new_channel);
        if(current.version.equals(channel.version)) {
            String sql = "UPDATE Channel SET" +
                " (version,name) =('" 
                                + updated.version  + "','"
                                + new_channel.name
                                + "') WHERE id='"+ updated.identity + "'";
            connection.createStatement().executeUpdate(sql);
            
            
            // Rewrite the list of events
            connection.createStatement().executeUpdate("DELETE FROM ChannelEvent WHERE channel='" + channel.identity + "'");
            
            final Maybe.Builder<SQLException> exception = Maybe.builder();
            final Mutable<Integer> ordinal = new Mutable<Integer>(0);
            new_channel.events.forEach(event -> {
                final String msql = "INSERT INTO ChannelEvent VALUES('" + channel.identity + "','"
                                                                            + event.identity + "','"
                                                                            + ordinal.get().toString() + "')";
                try { connection.createStatement().executeUpdate(msql); }
                catch (SQLException e) { exception.accept(e) ; }
                ordinal.accept(ordinal.get() + 1);
            });

            Util.throwMaybe(exception.getMaybe());
        } else {
            throw new UpdatedException(current);
        }
        giveNextVersion(updated);
        return updated;
    }
   
    @Override
    public synchronized void delete(Stored<Channel> channel)
       throws UpdatedException,
              DeletedException,
              SQLException {
        final Stored<Channel> current = get(channel.identity);
        if(current.version.equals(channel.version)) {
        String sql =  "DELETE FROM Channel WHERE id ='" + channel.identity + "'";
        connection.createStatement().executeUpdate(sql);
        } else {
        throw new UpdatedException(current);
        }
    }
    @Override
    public Stored<Channel> get(UUID id)
      throws DeletedException,
             SQLException {

        final String channelsql = "SELECT version,name FROM Channel WHERE id = '" + id.toString() + "'";
        final String eventsql = "SELECT event,ordinal FROM ChannelEvent WHERE channel = '" + id.toString() + "' ORDER BY ordinal DESC";

        final Statement channelStatement = connection.createStatement();
        final Statement eventStatement = connection.createStatement();

        final ResultSet channelResult = channelStatement.executeQuery(channelsql);
        final ResultSet eventResult = eventStatement.executeQuery(eventsql);

        if(channelResult.next()) {
            final UUID version = 
                UUID.fromString(channelResult.getString("version"));
            final String name =
                channelResult.getString("name");
            // Get all the events associated with this channel
            final List.Builder<Stored<Channel.Event>> events = List.builder();
            while(eventResult.next()) {
                final UUID eventId = UUID.fromString(eventResult.getString("event"));
                events.accept(eventStore.get(eventId));
            }
            return (new Stored<Channel>(new Channel(name,events.getList()),id,version));
        } else {
            throw new DeletedException();
        }
    }
    
    public Stored<Channel> noChangeUpdate(UUID channelId)
        throws SQLException, DeletedException {
        String sql = "UPDATE Channel SET" +
                " (version) =('" + UUID.randomUUID() + "') WHERE id='"+ channelId + "'";
        connection.createStatement().executeUpdate(sql);
        Stored<Channel> channel = get(channelId);
        giveNextVersion(channel);
        return channel;
    }
    
    public UUID getCurrentVersion(UUID id)
      throws DeletedException,
             SQLException {

        final String channelsql = "SELECT version FROM Channel WHERE id = '" + id.toString() + "'";
        final Statement channelStatement = connection.createStatement();

        final ResultSet channelResult = channelStatement.executeQuery(channelsql);
        if(channelResult.next()) {
            return UUID.fromString(
                    channelResult.getString("version"));
        }
        throw new DeletedException();
    }
    
    /**
     * Wait for a new version of a channel
     **/
    public Stored<Channel> waitNextVersion(UUID identity, UUID version)
      throws DeletedException,
             SQLException {
        Maybe.Builder<Stored<Channel>> result
            = Maybe.builder();
        // Insert our result consumer
        synchronized(waiters) {
            Maybe<List<Consumer<Stored<Channel>>>> channelWaiters 
                = Maybe.just(waiters.get(identity));
            waiters.put(identity,List.cons(result,channelWaiters.defaultValue(List.empty())));
        }
        // Test if there already is a new version avaiable
        if(!getCurrentVersion(identity).equals( version)) {
            return get(identity);
        }
        // Wait
        synchronized(result) {
            while(true) {
                try {
                    result.wait();
                    return result.getMaybe().get();
                } catch (InterruptedException e) {
                    System.err.println("Thread interrupted.");
                } catch (Maybe.NothingException e) {
                    // Still no result, looping
                }
            }
        }
    }
    
    /**
     * Notify all waiters of a new version
     **/
    private void giveNextVersion(Stored<Channel> channel) {
        synchronized(waiters) {
            Maybe<List<Consumer<Stored<Channel>>>> channelWaiters 
                = Maybe.just(waiters.get(channel.identity));
            try {
                channelWaiters.get().forEach(w -> {
                    w.accept(channel);
                    synchronized(w) {
                        w.notifyAll();
                    }
                });
            } catch (Maybe.NothingException e) {
                // No were waiting for us :'(
            }
            waiters.put(channel.identity,List.empty());
        }
    }
    
    /**
     * Get the channel belonging to a specific event.
     */
    public Stored<Channel> lookupChannelForEvent(Stored<Channel.Event> e)
      throws SQLException, DeletedException {
        String sql = "SELECT channel FROM ChannelEvent WHERE event='" + e.identity + "'";
        final ResultSet rs = connection.createStatement().executeQuery(sql);
        if(rs.next()) {
            final UUID channelId = UUID.fromString(rs.getString("channel"));
            return get(channelId);
        }
        throw new DeletedException();
    }
} 
 
 
