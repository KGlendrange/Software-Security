package inf226.inchat;

import java.sql.Connection;
import java.sql.PreparedStatement;
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
        
        /* String sql =  "INSERT INTO Event VALUES('" + stored.identity + "','"
                                                  + stored.version  + "','"
                                                  + event.type.code + "','"
                                                  + event.time  + "')"; */
        String sql = "INSERT INTO Event VALUES (?,?,?,?)";
        try{
            PreparedStatement stmt = connection.prepareStatement(sql);
            stmt.setString(1,stored.identity.toString());
            stmt.setString(2,stored.version.toString());
            stmt.setInt(3,event.type.code);
            stmt.setString(4,event.time.toString());

            /* connection.createStatement().executeUpdate(sql); */
            stmt.executeUpdate();
        }catch(SQLException e){
            System.err.println("error: "+e);
        }
        try{
            PreparedStatement switchStmt = null;
            switch (event.type) {
                case message:
                    /* sql = "INSERT INTO Message VALUES('" + stored.identity + "','"
                                                        + event.sender + "','"
                                                        + event.message +"')"; */
                    sql = "INSERT INTO Message VALUES (?,?,?)";
                    switchStmt = connection.prepareStatement(sql);
                    switchStmt.setString(1,stored.identity.toString());
                    switchStmt.setString(2,event.sender);
                    switchStmt.setString(3,event.message);
                    break;
                case join:
                    /* sql = "INSERT INTO Joined VALUES('" + stored.identity + "','"
                                                    + event.sender +"')"; */
                    sql = "INSERT INTO Joined VALUES (?,?)";
                    switchStmt = connection.prepareStatement(sql);
                    switchStmt.setString(1,stored.identity.toString());
                    switchStmt.setString(2,event.sender);
                    break;
            }
            switchStmt.executeUpdate();
        }catch(SQLException e){
            System.err.println("error: "+e);
        }
        

        /* connection.createStatement().executeUpdate(sql); */
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
        /* String sql = "UPDATE Event SET" +
            " (version,time,type) =('" 
                            + updated.version  + "','"
                            + new_event.time  + "','"
                            + new_event.type.code
                            + "') WHERE id='"+ updated.identity + "'"; */
        String sql = "UPDATE Event SET (version,time,type) = (?,?,?) WHERE id= ?";
        PreparedStatement stmt = connection.prepareStatement(sql);
        stmt.setString(1,updated.version.toString());
        stmt.setString(2,new_event.time.toString());
        stmt.setInt(3,new_event.type.code);
        stmt.setString(4,updated.identity.toString());
        stmt.executeUpdate();
        /* connection.createStatement().executeUpdate(sql); */
        PreparedStatement switchStmt = null;
        switch (new_event.type) {
            case message:
                /* sql = "UPDATE Message SET (sender,content)=('" + new_event.sender + "','"
                                                     + new_event.message +"') WHERE id='"+ updated.identity + "'"; */
                sql = "UPDATE Message SET (sender,content)=(?,?) WHERE id= ?";
                switchStmt = connection.prepareStatement(sql);
                switchStmt.setString(1,new_event.sender);
                switchStmt.setString(2,new_event.message);
                switchStmt.setString(3,updated.identity.toString());
                break;
            case join:
                /* sql = "UPDATE Joined SET (sender)=('" + new_event.sender +"') WHERE id='"+ updated.identity + "'"; */
                sql = "UPDATE Joined SET (sender)=(?) WHERE id= ?";
                switchStmt = connection.prepareStatement(sql);
                switchStmt.setString(1,new_event.sender);
                switchStmt.setString(2,updated.identity.toString());
                break;
        }
        /* connection.createStatement().executeUpdate(sql); */
        switchStmt.executeUpdate();
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
        /* String sql =  "DELETE FROM Event WHERE id ='" + event.identity + "'"; */
        String sql = "DELETE FROM Event WHERE id= ?";
        PreparedStatement stmt = connection.prepareStatement(sql);
        stmt.setString(1,event.identity.toString());
        /* connection.createStatement().executeUpdate(sql); */
        stmt.executeUpdate();
        } else {
        throw new UpdatedException(current);
        }
    }
    @Override
    public Stored<Channel.Event> get(UUID id)
      throws DeletedException,
             SQLException {
        /* final String sql = "SELECT version,time,type FROM Event WHERE id = '" + id.toString() + "'"; */
        final String sql = "SELECT  version,time,type FROM Event WHERE id = ?";

        /* final Statement statement = connection.createStatement(); */
        PreparedStatement stmt = connection.prepareStatement(sql);
        stmt.setString(1,id.toString());
        /* final ResultSet rs = statement.executeQuery(sql); */
        final ResultSet rs = stmt.executeQuery();

        if(rs.next()) {
            final UUID version = UUID.fromString(rs.getString("version"));
            final Channel.Event.Type type = 
                Channel.Event.Type.fromInteger(rs.getInt("type"));
            final Instant time = 
                Instant.parse(rs.getString("time"));
            
            /* final Statement mstatement = connection.createStatement(); */
            PreparedStatement mStatement = null;
            switch(type) {
                case message:
                    /* final String msql = "SELECT sender,content FROM Message WHERE id = '" + id.toString() + "'"; */
                    final String msql = "SELECT sender,content FROM Message WHERE id = ?";
                    mStatement = connection.prepareStatement(msql);
                    mStatement.setString(1,id.toString());
                    
                    /* final ResultSet mrs = mstatement.executeQuery(msql); */
                    final ResultSet mrs = mStatement.executeQuery();
                    mrs.next();
                    return new Stored<Channel.Event>(
                            Channel.Event.createMessageEvent(time,mrs.getString("sender"),mrs.getString("content")),
                            id,
                            version);
                case join:
                    /* final String asql = "SELECT sender FROM Joined WHERE id = '" + id.toString() + "'"; */
                    final String asql = "SELECT sender FROM Joined WHERE id = ?";
                    mStatement = connection.prepareStatement(asql);
                    mStatement.setString(1,id.toString());
                    /* final ResultSet ars = mstatement.executeQuery(asql); */
                    final ResultSet ars = mStatement.executeQuery();
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


 
