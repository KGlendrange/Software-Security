package inf226.inchat;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.UUID;

import inf226.storage.*;

import inf226.util.immutable.List;
import inf226.util.*;

import java.sql.PreparedStatement;

public final class AccountStorage
    implements Storage<Account,SQLException> {
    
    final Connection connection;
    final Storage<User,SQLException> userStore;
    final Storage<Channel,SQLException> channelStore;
    
    public AccountStorage(Connection connection,
                          Storage<User,SQLException> userStore,
                          Storage<Channel,SQLException> channelStore) 
      throws SQLException {
        this.connection = connection;
        this.userStore = userStore;
        this.channelStore = channelStore;
        
        connection.createStatement()
                .executeUpdate("CREATE TABLE IF NOT EXISTS Account (id TEXT PRIMARY KEY, version TEXT, user TEXT, hashed TEXT, FOREIGN KEY(user) REFERENCES User(id) ON DELETE CASCADE)");
        connection.createStatement()
                .executeUpdate("CREATE TABLE IF NOT EXISTS AccountChannel (account TEXT, channel TEXT, alias TEXT, role TEXT, ordinal INTEGER, PRIMARY KEY(account,channel), FOREIGN KEY(account) REFERENCES Account(id) ON DELETE CASCADE, FOREIGN KEY(channel) REFERENCES Channel(id) ON DELETE CASCADE)");
    }
    
    @Override
    public Stored<Account> save(Account account)
      throws SQLException {
        
        final Stored<Account> stored = new Stored<Account>(account);

        final String myQuery = "INSERT INTO Account VALUES (?,?,?,?)";
        
        PreparedStatement myStmt = connection.prepareStatement(myQuery);
        myStmt.setString(1,stored.identity.toString());
        myStmt.setString(2,stored.version.toString());
        myStmt.setString(3,account.user.identity.toString());
        myStmt.setString(4,account.hashed.toString());

        myStmt.executeUpdate();
        
        // Write the list of channels
        final Maybe.Builder<SQLException> exception = Maybe.builder();
        final Mutable<Integer> ordinal = new Mutable<Integer>(0);
        account.channels.forEach(element -> {
            String alias = element.first;
            String role = element.second;
            Stored<Channel> channel = element.third;
            /* final String msql
              = "INSERT INTO AccountChannel VALUES('" + stored.identity + "','"
                                                      + channel.identity + "','"
                                                      + alias + "','"
                                                      + ordinal.get().toString() + "')"; */
            final String msql = "INSERT INTO AccountChannel VALUES (?,?,?,?,?)";

            try { 
                PreparedStatement mStmt = connection.prepareStatement(msql);
                mStmt.setString(1,stored.identity.toString());
                mStmt.setString(2,channel.identity.toString());
                mStmt.setString(3,alias);
                mStmt.setString(4,role);
                mStmt.setString(5,ordinal.get().toString());
                mStmt.executeUpdate(); 
            }
            catch (SQLException e) { exception.accept(e) ; }
            ordinal.accept(ordinal.get() + 1);
        });

        Util.throwMaybe(exception.getMaybe());
        return stored;
    }
    
    @Override
    public synchronized Stored<Account> update(Stored<Account> account,
                                            Account new_account)
        throws UpdatedException,
            DeletedException,
            SQLException {
    final Stored<Account> current = get(account.identity);
    final Stored<Account> updated = current.newVersion(new_account);
    if(current.version.equals(account.version)) {
        /* String sql = "UPDATE Account SET" +
            " (version,user) =('" 
                            + updated.version  + "','"
                            + new_account.user.identity
                            + "') WHERE id='"+ updated.identity + "'"; */

        final String sql = "UPDATE Account SET (version,user,hashed) = (?,?,?) WHERE id = ?";
        
        PreparedStatement stmt = connection.prepareStatement(sql);
        stmt.setString(1,updated.version.toString());
        stmt.setString(2,new_account.user.identity.toString());
        stmt.setString(3,new_account.hashed.toString());
        stmt.setString(4,updated.identity.toString());

        stmt.executeUpdate();
        
        
        // Rewrite the list of channels
        /* connection.createStatement().executeUpdate("DELETE FROM AccountChannel WHERE account='" + account.identity + "'"); */
        PreparedStatement mstmt = connection.prepareStatement("DELETE FROM AccountChannel WHERE Account = ?");
        mstmt.setString(1,account.identity.toString());
        mstmt.executeUpdate();
        
        final Maybe.Builder<SQLException> exception = Maybe.builder();
        final Mutable<Integer> ordinal = new Mutable<Integer>(0);
        new_account.channels.forEach(element -> {
            String alias = element.first;
            String role = element.second;
            Stored<Channel> channel = element.third;
            /* final String msql
                = "INSERT INTO AccountChannel VALUES('" + account.identity + "','"
                                                        + channel.identity + "','"
                                                        + alias + "','"
                                                        + ordinal.get().toString() + "')"; */
            
            final String msql = "INSERT INTO AccountChannel VALUES (?,?,?,?,?)";

            
            
            try { 
                PreparedStatement mStmt = connection.prepareStatement(msql);
                mStmt.setString(1,account.identity.toString());
                mStmt.setString(2,channel.identity.toString());
                mStmt.setString(3,alias);
                mStmt.setString(4,role);
                mStmt.setString(5,ordinal.get().toString());
                mStmt.executeUpdate(); 
            }
            catch (SQLException e) { exception.accept(e) ; }
            ordinal.accept(ordinal.get() + 1);
        });

        Util.throwMaybe(exception.getMaybe());
    } else {
        throw new UpdatedException(current);
    }
    return updated;
    }
    
    @Override
    public synchronized void delete(Stored<Account> account)
       throws UpdatedException,
              DeletedException,
              SQLException {
        final Stored<Account> current = get(account.identity);
        if(current.version.equals(account.version)) {

        //String sql =  "DELETE FROM Account WHERE id ='" + account.identity + "'";
        String sql = "DELETE FROM Account WHERE id = ?";
        
        PreparedStatement stmt = connection.prepareStatement(sql);
        stmt.setString(1,account.identity.toString());
        stmt.executeUpdate();

        } else {
        throw new UpdatedException(current);
        }
    }
    @Override
    public Stored<Account> get(UUID id)
      throws DeletedException,
             SQLException {
        //final String accountsql = "SELECT version,user FROM Account WHERE id = '" + id.toString() + "'";
        final String accountsql = "SELECT version, user, hashed FROM Account WHERE id = ?";
         
        //final String channelsql = "SELECT channel,alias,ordinal FROM AccountChannel WHERE account = '" + id.toString() + "' ORDER BY ordinal DESC";
        final String channelsql = "SELECT channel,alias,role,ordinal FROM AccountChannel WHERE account = ? ORDER BY ordinal DESC";

        //final Statement accountStatement = connection.createStatement();
        PreparedStatement accountStatement = connection.prepareStatement(accountsql);
        accountStatement.setString(1,id.toString());

        //final Statement channelStatement = connection.createStatement();
        PreparedStatement channelStatement = connection.prepareStatement(channelsql);
        channelStatement.setString(1,id.toString());

        //final ResultSet accountResult = accountStatement.executeQuery(accountsql);
        final ResultSet accountResult = accountStatement.executeQuery();
        //final ResultSet channelResult = channelStatement.executeQuery(channelsql);
        final ResultSet channelResult = channelStatement.executeQuery();


        if(accountResult.next()) {
            final UUID version = UUID.fromString(accountResult.getString("version"));
            final UUID userid =
            UUID.fromString(accountResult.getString("user"));
            final Stored<User> user = userStore.get(userid);

            final Password hashed = new Password(accountResult.getString("hashed"));

            // Get all the channels associated with this account
            final List.Builder<Triple<String,String,Stored<Channel>>> channels = List.builder();
            while(channelResult.next()) {
                final UUID channelId = 
                    UUID.fromString(channelResult.getString("channel"));
                final String alias = channelResult.getString("alias");
                final String role = channelResult.getString("role");
                
                channels.accept(
                    new Triple<String,String,Stored<Channel>>(
                        alias,role,channelStore.get(channelId)));
            }
            return (new Stored<Account>(new Account(user,channels.getList(),hashed),id,version));
        } else {
            throw new DeletedException();
        }
    }
    
    public Stored<Account> lookup(String username)
      throws DeletedException,
             SQLException {
        //final String sql = "SELECT Account.id from Account INNER JOIN User ON user=User.id where User.name='" + username + "'";
        final String sql = "SELECT Account.id from Account INNER JOIN User ON user=User.id where User.name = ?";
        PreparedStatement stmt = null;
        try{
            stmt = connection.prepareStatement(sql);

            stmt.setString(1,username);

            final ResultSet rs = stmt.executeQuery();



            if(rs.next()) {
                final UUID identity = 
                        UUID.fromString(rs.getString("id"));
                return get(identity);
            }

        }catch(SQLException e){
            System.err.println("Error: "+ e.getMessage());
        }
        
        
        throw new DeletedException();
    }
    public int getOwnerCount(Stored<Channel> channel, Stored<Account> account) throws SQLException, DeletedException {
        System.err.println("amount of owners");
        String sql = "SELECT COUNT('role') FROM AccountChannel WHERE (account = ? AND channel = ?) AND role= 'owner'";
        PreparedStatement stmt = connection.prepareStatement(sql);
        stmt.setString(1,account.identity.toString());
        stmt.setString(2,channel.identity.toString());
        final int rs = stmt.executeQuery().getInt(1);
        System.err.println(rs + "amount of owners");
        return rs;

    }
    /**
     * Get the account role belonging to a specific channel.
     */
    public String lookupRoleInChannel(Stored<Channel> channel, Stored<Account> account)
      throws SQLException, DeletedException {
        String sql = "SELECT role FROM AccountChannel WHERE (account = ? AND channel = ?)";
        PreparedStatement stmt = connection.prepareStatement(sql);
        stmt.setString(1,account.identity.toString());
        stmt.setString(2,channel.identity.toString());
        final ResultSet rs = stmt.executeQuery();
        if(rs.next()) {
            final String  role = rs.getString("role");
            return role;
        }
        throw new DeletedException();
    }
} 


 
