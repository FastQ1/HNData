public class Main {

    public static void main(String[] args) throws ClassNotFoundException {
        TempDB.Database db;

        Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");

        if (args.length == 0) {
            System.out.println("Defaulting to Microsoft SQL Server");
            db = TempDB.Database.MSSQLSERVER;
        } else {
            if (args.length != 1) throw new UnsupportedOperationException("Unsupported input- too many arguments");
            String s = args[0];
            switch (s) {
//                case "sqlitevm":
////                    db = TempDB.Database.SQLITE_VM;
////                    System.out.println("SQLite on VM selected");
////                    break;
//                    throw new UnsupportedOperationException();
                case "sqlitehome":
                    db = TempDB.Database.SQLITE_HOME;
                    System.out.println("SQLite on desktop selected");
                    break;
                case "msqlserver":
                    db = TempDB.Database.MSSQLSERVER;
                    System.out.println("Microsoft SQL Server selected");
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported input: field not recognized. Options: msqlserver | sqlitehome");
            }
        }

        //Uses a singleton even though there isn't any risk of creating multiple instances with current implementation
        //the TempDB constructor can only exit via an exception which will terminate program
        TempDB.getInstance(db);
    }
}