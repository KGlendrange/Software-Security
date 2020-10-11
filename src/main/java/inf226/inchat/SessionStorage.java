package inf226.inchat;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.UUID;

import inf226.storage.*;

public final class SessionStorage
    implements Storage<Session,SQLException> {
    
    final Connection connection;
    final Storage<Account,SQLException> accountStorage;
    
    public SessionStorage(Connection connection,
                          Storage<Account,SQLException> accountStorage)
      throws SQLException {
        this.connection = connection;
        this.accountStorage = accountStorage;
        connection.createStatement()
                .executeUpdate("CREATE TABLE IF NOT EXISTS Session (id TEXT PRIMARY KEY, version TEXT, account TEXT, expiry TEXT, FOREIGN KEY(account) REFERENCES Account(id) ON DELETE CASCADE)");
    }
    
    @Override
    public Stored<Session> save(Session session)
      throws SQLException {
        
        final Stored<Session> stored = new Stored<Session>(session);
        /* String sql =  "INSERT INTO Session VALUES('" + stored.identity + "','"
                                                  + stored.version  + "','"
                                                  + session.account.identity  + "','"
                                                  + session.expiry.toString() + "')"; */
        String sql = "INSERT INTO Session VALUES (?,?,?,?)";
        PreparedStatement stmt = connection.prepareStatement(sql);
        stmt.setString(1, stored.identity.toString());
        stmt.setString(2, stored.version.toString());
        stmt.setString(3,session.account.identity.toString());
        stmt.setString(4,session.expiry.toString());

        stmt.executeUpdate();

        /* connection.createStatement().executeUpdate(sql); */
        return stored;
    }
    
    @Override
    public synchronized Stored<Session> update(Stored<Session> session,
                                            Session new_session)
        throws UpdatedException,
            DeletedException,
            SQLException {
    final Stored<Session> current = get(session.identity);
    final Stored<Session> updated = current.newVersion(new_session);
    if(current.version.equals(session.version)) {
        /* String sql = "UPDATE Session SET" +
            " (version,account,expiry) =('" 
                            + updated.version  + "','"
                            + new_session.account.identity  + "','"
                            + new_session.expiry.toString()
                            + "') WHERE id='"+ updated.identity + "'"; */
        String sql = "UPDATE Session SET (version,account,expiry) = (?,?,?) WHERE id= ?";
        PreparedStatement stmt = connection.prepareStatement(sql);
        stmt.setString(1,updated.version.toString());
        stmt.setString(2,new_session.account.identity.toString());
        stmt.setString(3,new_session.expiry.toString());
        stmt.setString(4,updated.identity.toString());

        stmt.executeUpdate();
        /* connection.createStatement().executeUpdate(sql); */
    } else {
        throw new UpdatedException(current);
    }
    return updated;
    }
   
    @Override
    public synchronized void delete(Stored<Session> session)
       throws UpdatedException,
              DeletedException,
              SQLException {
        final Stored<Session> current = get(session.identity);
        if(current.version.equals(session.version)) {
        /* String sql =  "DELETE FROM Session WHERE id ='" + session.identity + "'"; */
        String sql = "DELETE FROM Session WHERE id = ?";
        PreparedStatement stmt = connection.prepareStatement(sql);
        stmt.setString(1,session.identity.toString());
        /* connection.createStatement().executeUpdate(sql); */
        stmt.executeUpdate();
        } else {
        throw new UpdatedException(current);
        }
    }
    @Override
    public Stored<Session> get(UUID id)
      throws DeletedException,
             SQLException {
        /* final String sql = "SELECT version,account,expiry FROM Session WHERE id = '" + id.toString() + "'"; */
        final String sql = "SELECT version,account,expiry FROM Session WHERE id = ?";

        /* final Statement statement = connection.createStatement();
        final ResultSet rs = statement.executeQuery(sql); */
        PreparedStatement stmt = connection.prepareStatement(sql);
        stmt.setString(1,id.toString());
        final ResultSet rs = stmt.executeQuery();

        if(rs.next()) {
            final UUID version = UUID.fromString(rs.getString("version"));
            final Stored<Account> account
               = accountStorage.get(
                    UUID.fromString(rs.getString("account")));
            final Instant expiry = Instant.parse(rs.getString("expiry"));
            return (new Stored<Session>
                        (new Session(account,expiry),id,version));
        } else {
            throw new DeletedException();
        }
    }
    
    
} 
