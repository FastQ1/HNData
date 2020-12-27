import org.apache.commons.lang3.exception.ExceptionUtils;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

public class Logger {

    private final Connection loggerConn;

    public Logger(String connectionString) throws SQLException {
        if (connectionString.equals(ConnectionVariables.getSqliteHomeConnectionString()))
            connectionString = ConnectionVariables.getSqliteHomeConnectionStringLogger();
        else if (!connectionString.equals(ConnectionVariables.getSqlServerConnectionString()))
            throw new UnsupportedOperationException("Problem choosing the connection string");

        loggerConn = DriverManager.getConnection(connectionString);
        initialize();

    }

    public void bug(Exception bug) throws SQLException {
        String desc;
        if (bug.getMessage() != null) {
            desc = clean(bug.getMessage());

        } else desc = "";
        String stacktrace = ExceptionUtils.getStackTrace(bug);


        StringBuilder sb = new StringBuilder("INSERT INTO events (event, timeStamp, description) VALUES('exception', ");
        sb.append("'").append(getTime()).append("'").append(",")
                .append("'").append(desc).append("\n").append(stacktrace).append("');");

        execute(sb.toString());

    }

    public void cycle() throws SQLException {
        String s = "INSERT INTO events (event, timeStamp) VALUES('cycle', '" + getTime() + "');";
        execute(s);
    }

    public void reconcile(Post post, boolean newPost) throws SQLException {
        StringBuilder description = new StringBuilder(clean(post.getTitle()) + "\n");
        String event = newPost ? "reconcileNew" : "reconcileDuplicate";
        description.append(post.getCommentLink());

        StringBuilder sb = new StringBuilder("INSERT INTO events (event, timeStamp, description) VALUES('" + event + "', ");
        sb.append("'").append(getTime()).append("'").append(",")
                .append("'").append(description).append("');");

        execute(sb.toString());

    }

    public void initialize() throws SQLException {
        String s = "INSERT INTO events (event, timeStamp) VALUES('loggerStart', '" + getTime() + "');";
        execute(s);
    }

    public void query(String message) throws SQLException {
        String s = "INSERT INTO queries (query, timeStamp) VALUES ('" + message + "', '" + getTime() + "');";
        execute(s);
    }

    //fill out when I need this
    public void otherEvent(String description) {
    }

    public void close() throws SQLException {
        loggerConn.close();
    }

    private String getTime() {
        ZonedDateTime timeToFormat = ZonedDateTime.now(ZoneId.of("Z"));
        return DateTimeFormatter.ISO_DATE_TIME.format(timeToFormat);
    }

    private String clean(String s) {
        return s.replace("'", "''");
    }

    private void execute(String s) throws SQLException {
        PreparedStatement statement = loggerConn.prepareStatement(s);
        statement.execute();
    }


}
