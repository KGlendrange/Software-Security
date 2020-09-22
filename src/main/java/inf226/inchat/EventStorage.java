package inf226.inchat;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.UUID;
import java.util.function.Consumer;

import inf226.storage.*;
import inf226.util.*;




public final class EventStorage
    implements Storage<Channel.Event,SQLException> {
    
    private final Connection connection;
    
    public EventStorage(Connection connection) 
      throws SQLException {
        this.connection = connection;
        connection.createStatement()
                .executeUpdate("CREATE TABLE IF NOT EXISTS Event (id TEXT PRIMARY KEY, version TEXT, type INTEGER, time TEXT)");
        connection.createStatement()
                .executeUpdate("CREATE TABLE IF NOT EXISTS Message (id TEXT PRIMARY KEY, sender TEXT, content Text, FOREIGN KEY(id) REFERENCES Event(id) ON DELETE CASCADE)");
        connection.createStatement()
                .executeUpdate("CREATE TABLE IF NOT EXISTS Joined (id TEXT PRIMARY KEY, sender TEXT, FOREIGN KEY(id) REFERENCES Event(id) ON DELETE CASCADE)");
    }
    
    @Override
    public Stored<Channel.Event> save(Channel.Event event)
      throws SQLException {
        
        final Stored<Channel.Event> stored = new Stored<Channel.Event>(event);
        
        String sql =  "INSERT INTO Event VALUES('" + stored.identity + "','"
                                                  + stored.version  + "','"
                                                  + event.type.code + "','"
                                                  + event.time  + "')";
        connection.createStatement().executeUpdate(sql);
        switch (event.type) {
            case message:
                sql = "INSERT INTO Message VALUES('" + stored.identity + "','"
                                                     + event.sender + "','"
                                                     + event.message +"')";
                break;
            case join:
                sql = "INSERT INTO Joined VALUES('" + stored.identity + "','"
                                                  + event.sender +"')";
                break;
        }
        connection.createStatement().executeUpdate(sql);
        return stored;
    }
    
    @Override
    public synchronized Stored<Channel.Event> update(Stored<Channel.Event> event,
                                            Channel.Event new_event)
        throws UpdatedException,
            DeletedException,
            SQLException {
    final Stored<Channel.Event> current = get(event.identity);
    final Stored<Channel.Event> updated = current.newVersion(new_event);
    if(current.version.equals(event.version)) {
        String sql = "UPDATE Event SET" +
            " (version,time,type) =('" 
                            + updated.version  + "','"
                            + new_event.time  + "','"
                            + new_event.type.code
                            + "') WHERE id='"+ updated.identity + "'";
        connection.createStatement().executeUpdate(sql);
        switch (new_event.type) {
            case message:
                sql = "UPDATE Message SET (sender,content)=('" + new_event.sender + "','"
                                                     + new_event.message +"') WHERE id='"+ updated.identity + "'";
                break;
            case join:
                sql = "UPDATE Joined SET (sender)=('" + new_event.sender +"') WHERE id='"+ updated.identity + "'";
                break;
        }
        connection.createStatement().executeUpdate(sql);
    } else {
        throw new UpdatedException(current);
    }
        return updated;
    }
   
    @Override
    public synchronized void delete(Stored<Channel.Event> event)
       throws UpdatedException,
              DeletedException,
              SQLException {
        final Stored<Channel.Event> current = get(event.identity);
        if(current.version.equals(event.version)) {
        String sql =  "DELETE FROM Event WHERE id ='" + event.identity + "'";
        connection.createStatement().executeUpdate(sql);
        } else {
        throw new UpdatedException(current);
        }
    }
    @Override
    public Stored<Channel.Event> get(UUID id)
      throws DeletedException,
             SQLException {
        final String sql = "SELECT version,time,type FROM Event WHERE id = '" + id.toString() + "'";
        final Statement statement = connection.createStatement();
        final ResultSet rs = statement.executeQuery(sql);

        if(rs.next()) {
            final UUID version = UUID.fromString(rs.getString("version"));
            final Channel.Event.Type type = 
                Channel.Event.Type.fromInteger(rs.getInt("type"));
            final Instant time = 
                Instant.parse(rs.getString("time"));
            
            final Statement mstatement = connection.createStatement();
            switch(type) {
                case message:
                    final String msql = "SELECT sender,content FROM Message WHERE id = '" + id.toString() + "'";
                    final ResultSet mrs = mstatement.executeQuery(msql);
                    mrs.next();
                    return new Stored<Channel.Event>(
                            Channel.Event.createMessageEvent(time,mrs.getString("sender"),mrs.getString("content")),
                            id,
                            version);
                case join:
                    final String asql = "SELECT sender FROM Joined WHERE id = '" + id.toString() + "'";
                    final ResultSet ars = mstatement.executeQuery(asql);
                    ars.next();
                    return new Stored<Channel.Event>(
                            Channel.Event.createJoinEvent(time,ars.getString("sender")),
                            id,
                            version);
            }
        }
        throw new DeletedException();
    }
    
}


 
