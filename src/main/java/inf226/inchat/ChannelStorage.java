package inf226.inchat;

import java.sql.Connection;
import java.sql.PreparedStatement;
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
import java.util.Arrays;

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
        connection.createStatement()
                .executeUpdate("CREATE TABLE IF NOT EXISTS Permissions (channel TEXT, user TEXT, role TEXT, PRIMARY KEY(channel,user), FOREIGN KEY(channel) REFERENCES Channel(id) ON DELETE CASCADE, FOREIGN KEY(user) REFERENCES User(id) ON DELETE CASCADE)");
        }
    
    @Override
    public Stored<Channel> save(Channel channel)
      throws SQLException {
        
        final Stored<Channel> stored = new Stored<Channel>(channel);
        /* String sql =  "INSERT INTO Channel VALUES('" + stored.identity + "','"
                                                  + stored.version  + "','"
                                                  + channel.name  + "')"; */
        String sql = "INSERT INTO Channel VALUES (?,?,?)";
        
        /* connection.createStatement().executeUpdate(sql); */
        PreparedStatement stmt = connection.prepareStatement(sql);
        stmt.setString(1,stored.identity.toString());
        stmt.setString(2,stored.version.toString());
        stmt.setString(3,channel.name);

        stmt.executeUpdate();


        System.err.println("channel.permissionList: ");
        channel.permissionList.forEach(element -> {
            System.err.println("elem: "+element);
            Stored<User> user = element.first;
            String role = element.second;

            final String permsql = "INSERT INTO Permissions VALUES (?,?,?)";
            try{
                PreparedStatement permstmt = connection.prepareStatement(permsql);
                permstmt.setString(1,stored.identity.toString());
                permstmt.setString(2,user.identity.toString());
                permstmt.setString(3,role);

                permstmt.executeUpdate();
            }catch(SQLException e){
                System.err.println("error: "+e);
            }
        });

        
        // Write the list of events
        final Maybe.Builder<SQLException> exception = Maybe.builder();
        final Mutable<Integer> ordinal = new Mutable<Integer>(0);
        channel.events.forEach(event -> {
            /* final String msql = "INSERT INTO ChannelEvent VALUES('" + stored.identity + "','"
                                                                        + event.identity + "','"
                                                                        + ordinal.get().toString() + "')"; */
            final String msql = "INSERT INTO ChannelEvent VALUES (?,?,?)";
            
            try { 
                /* connection.createStatement().executeUpdate(msql);  */
                PreparedStatement mStmt = connection.prepareStatement(msql);
                mStmt.setString(1,stored.identity.toString());
                mStmt.setString(2,event.identity.toString());
                mStmt.setString(3, ordinal.get().toString());

                mStmt.executeUpdate();
            }
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
            /* String sql = "UPDATE Channel SET" +
                " (version,name) =('" 
                                + updated.version  + "','"
                                + new_channel.name
                                + "') WHERE id='"+ updated.identity + "'"; */

            String sql = "UPDATE Channel SET (version,name) = (?,?) WHERE id = ?";
            
            /* connection.createStatement().executeUpdate(sql); */
            PreparedStatement stmt = connection.prepareStatement(sql);
            stmt.setString(1,updated.version.toString());
            stmt.setString(2,new_channel.name);
            stmt.setString(3,updated.identity.toString());
            stmt.executeUpdate();

            
            
            // Rewrite the list of events
            /* connection.createStatement().executeUpdate("DELETE FROM ChannelEvent WHERE channel='" + channel.identity + "'"); */
            PreparedStatement myStmt = connection.prepareStatement("DELETE FROM ChannelEvent WHERE channel= ?");
            myStmt.setString(1,channel.identity.toString());
            myStmt.executeUpdate();
            
            final Maybe.Builder<SQLException> exception = Maybe.builder();
            final Mutable<Integer> ordinal = new Mutable<Integer>(0);
            new_channel.events.forEach(event -> {
                /* final String msql = "INSERT INTO ChannelEvent VALUES('" + channel.identity + "','"
                                                                            + event.identity + "','"
                                                                            + ordinal.get().toString() + "')"; */
                final String msql = "INSERT INTO ChannelEvent VALUES (?,?,?)";

                try { 
                    /* connection.createStatement().executeUpdate(msql);  */
                    PreparedStatement mStmt = connection.prepareStatement(msql);
                    mStmt.setString(1,channel.identity.toString());
                    mStmt.setString(2,event.identity.toString());
                    mStmt.setString(3,ordinal.get().toString());
                    mStmt.executeUpdate();

                }
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
        /* String sql =  "DELETE FROM Channel WHERE id ='" + channel.identity + "'"; */
        String sql = "DELETE FROM Channel WHERE id = ?";
        PreparedStatement stmt = connection.prepareStatement(sql);
        stmt.setString(1,channel.identity.toString());
        stmt.executeUpdate();
        /* connection.createStatement().executeUpdate(sql); */
        } else {
        throw new UpdatedException(current);
        }
    }
    @Override
    public Stored<Channel> get(UUID id)
      throws DeletedException,
             SQLException {

        /* final String channelsql = "SELECT version,name FROM Channel WHERE id = '" + id.toString() + "'"; */
        final String channelsql = "SELECT version, name FROM Channel WHERE id = ?";

        /* final String eventsql = "SELECT event,ordinal FROM ChannelEvent WHERE channel = '" + id.toString() + "' ORDER BY ordinal DESC"; */
        final String eventsql = "SELECT event,ordinal FROM ChannelEvent WHERE channel = ? ORDER BY ordinal DESC";

        /* final Statement channelStatement = connection.createStatement();
        final Statement eventStatement = connection.createStatement(); */

        PreparedStatement channelStmt = connection.prepareStatement(channelsql);
        channelStmt.setString(1,id.toString());
        PreparedStatement eventStmt = connection.prepareStatement(eventsql);
        eventStmt.setString(1,id.toString());

        /* final ResultSet channelResult = channelStatement.executeQuery(channelsql);
        final ResultSet eventResult = eventStatement.executeQuery(eventsql); */
        final ResultSet channelResult = channelStmt.executeQuery();
        final ResultSet eventResult = eventStmt.executeQuery();

        final String permissionsql = "SELECT user,role FROM Permissions WHERE channel = ?";
        PreparedStatement permissionStmt = connection.prepareStatement(permissionsql);
        permissionStmt.setString(1,id.toString());
        

        final ResultSet permissionResult = permissionStmt.executeQuery();

        
        

        if(channelResult.next()) {
            final UUID version = 
                UUID.fromString(channelResult.getString("version"));
            final String name =
                channelResult.getString("name");

            /* //Get the permisisonList accociated with this channel
            final List.Builder<Pair<Stored<User>,String>> permissionList = List.builder(); */

            

            // Get all the events associated with this channel
            final List.Builder<Stored<Channel.Event>> events = List.builder();

        
            while(eventResult.next()) {
                final UUID eventId = UUID.fromString(eventResult.getString("event"));
                events.accept(eventStore.get(eventId));
            }
            if(permissionResult.next()){
                final String userString  = permissionResult.getString("user");
                System.err.println("userstring: "+userString);

/* 
                final Stored<User> new_user = new Stored<User>(user,id,version);
                final String role = permissionResult.getString("role");

                final Pair<Stored<User>,String> pair = new Pair<Stored<User>,String>(user,role);
    
                final List.Builder<Pair<Stored<User>,String>> permissionList = List.builder(); */

                return (new Stored<Channel>(new Channel(name,events.getList(),List.empty()),id,version));
            }else{
                return (new Stored<Channel>(new Channel(name,events.getList(),List.empty()),id,version));
            }
            
        } else {
            throw new DeletedException();
        }
    }
    
    public Stored<Channel> noChangeUpdate(UUID channelId)
        throws SQLException, DeletedException {
        /* String sql = "UPDATE Channel SET" +
                " (version) =('" + UUID.randomUUID() + "') WHERE id='"+ channelId + "'"; */
        String sql = "UPDATE Channel SET (version) = (?) WHERE id= ?";
        PreparedStatement stmt = connection.prepareStatement(sql);
        stmt.setString(1,channelId.toString());
        stmt.executeUpdate();

        /* connection.createStatement().executeUpdate(sql); */
        Stored<Channel> channel = get(channelId);
        giveNextVersion(channel);
        return channel;
    }
    
    public UUID getCurrentVersion(UUID id)
      throws DeletedException,
             SQLException {

        /* final String channelsql = "SELECT version FROM Channel WHERE id = '" + id.toString() + "'"; */
        final String channelsql = "SELECT version FROM Channel WHERE id = ?";
        PreparedStatement channelStmt = connection.prepareStatement(channelsql);
        channelStmt.setString(1, id.toString());
        
        /* final Statement channelStatement = connection.createStatement(); */

        /* final ResultSet channelResult = channelStatement.executeQuery(channelsql); */
        final ResultSet channelResult = channelStmt.executeQuery();
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
        /* String sql = "SELECT channel FROM ChannelEvent WHERE event='" + e.identity + "'"; */
        String sql = "SELECT channel FROM ChannelEvent WHERE event= ?";
        PreparedStatement stmt = connection.prepareStatement(sql);
        stmt.setString(1,e.identity.toString());
        /* final ResultSet rs = connection.createStatement().executeQuery(sql); */
        final ResultSet rs = stmt.executeQuery();
        if(rs.next()) {
            final UUID channelId = UUID.fromString(rs.getString("channel"));
            return get(channelId);
        }
        throw new DeletedException();
    }
} 
 
 
