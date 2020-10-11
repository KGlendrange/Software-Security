package inf226.inchat;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.UUID;

import inf226.storage.*;
import inf226.util.*;




public final class UserStorage
    implements Storage<User,SQLException> {
    
    final Connection connection;
    
    public UserStorage(Connection connection) 
      throws SQLException {
        this.connection = connection;
        
        System.err.println("creating USER Table"
            +connection.createStatement()
            .executeUpdate("CREATE TABLE IF NOT EXISTS User (id TEXT PRIMARY KEY, version TEXT, name TEXT, password TEXT, joined TEXT)"));
    }
    
    @Override
    public Stored<User> save(User user)
      throws SQLException {
        System.err.println("Trying to save user in UserStorage");
        final Stored<User> stored = new Stored<User>(user);
        System.err.println("Test--code 888"+stored);

        /* String sql =  "INSERT INTO User VALUES('" + stored.identity + "','"
                                                  + stored.version  + "','"
                                                  + user.name  + "','"
                                                  + user.password + "','"
                                                  + user.joined.toString() + "')"; */
        String sql = "INSERT INTO User (id, version, name, password, joined) "
        +"VALUES (?,?,?,?,?)";

        System.err.println("Test--code 789"+sql);
        PreparedStatement stmt = null;
        try{
            stmt = connection.prepareStatement(sql);
            stmt.setString(1,stored.identity.toString());
            stmt.setString(2,stored.version.toString());
            stmt.setString(3,user.name);
            stmt.setString(4,user.password);
            stmt.setString(5,user.joined.toString());

            stmt.executeUpdate();

        }catch(SQLException e){
            System.err.println("error: " + e.getMessage());
        }

        return stored;
    }
    
    @Override
    public synchronized Stored<User> update(Stored<User> user,
                                            User new_user)
        throws UpdatedException,
            DeletedException,
            SQLException {
        final Stored<User> current = get(user.identity);
        final Stored<User> updated = current.newVersion(new_user);
        if(current.version.equals(user.version)) {
        /* String sql = "UPDATE User SET" +
            " (version,name,joined) =('" 
                            + updated.version  + "','"
                            + new_user.name  + "','"
                            + new_user.password + "','"
                            + new_user.joined.toString()
                            + "') WHERE id='"+ updated.identity + "'"; */
            String sql = "UPDATE User SET" +
                " (version,name,password,joined) = (?,?,?,?)"
                + " WHERE id = ?";
            PreparedStatement stmt = null;
            try{
                stmt = connection.prepareStatement(sql);

                stmt.setString(1,updated.version.toString());
                stmt.setString(2,new_user.name);
                stmt.setString(3,new_user.password);
                stmt.setString(4,new_user.joined.toString());
                stmt.setString(5,updated.identity.toString());

                stmt.executeUpdate();
                
            }catch(SQLException e){
                System.err.println("error: " + e.getMessage());

            }
        } else {
            throw new UpdatedException(current);
        }
        return updated;
    }
   
    @Override
    public synchronized void delete(Stored<User> user)
       throws UpdatedException,
              DeletedException,
              SQLException {
        final Stored<User> current = get(user.identity);
        if(current.version.equals(user.version)) {
            /* String sql =  "DELETE FROM User WHERE id ='" + user.identity + "'"; */
            String sql = "DELETE FROM User WHERE id = ?";
            PreparedStatement stmt = null;

            try{
                stmt = connection.prepareStatement(sql);
                stmt.setString(1,user.identity.toString());
                stmt.executeUpdate();
            }catch(SQLException e){
                System.err.println("error: " + e.getMessage());
                
            }
        } else {
        throw new UpdatedException(current);
        }
    }
    @Override
    public Stored<User> get(UUID id)
      throws DeletedException,
             SQLException {

       /*  final String sql = "SELECT version,name,password,joined FROM User WHERE id = '" + id.toString() + "'"; */
        final String sql = "SELECT version,name,password,joined from User where id = ?";
        final PreparedStatement stmt = connection.prepareStatement(sql);
        stmt.setString(1,id.toString());

        /* final Statement statement = connection.createStatement();
        final ResultSet rs = statement.executeQuery(sql); */
        final ResultSet rs = stmt.executeQuery();

        if(rs.next()) {
            final UUID version = 
                UUID.fromString(rs.getString("version"));
            final String name = rs.getString("name");


            //Password
            final String password = rs.getString("password");

            final Instant joined = Instant.parse(rs.getString("joined"));
            
            return (new Stored<User>
                        (new User(name,password,joined),id,version));
        } else {
            throw new DeletedException();
        }
    }
    
    /**
     * Look up a user by their username;
     **/
    public Maybe<Stored<User>> lookup(String name) {
        /* final String sql = "SELECT id FROM User WHERE name = '" + name + "'"; */
        final String sql = "SELECT id FROM User WHERE name = ?";

        try{
            /* final Statement statement = connection.createStatement();
            final ResultSet rs = statement.executeQuery(sql); */
            final PreparedStatement stmt = connection.prepareStatement(sql);
            stmt.setString(1,name);
            final ResultSet rs = stmt.executeQuery();
            if(rs.next())
                return Maybe.just(
                    get(UUID.fromString(rs.getString("id"))));
        } catch (Exception e) {
        
        }
        return Maybe.nothing();
    }
}


