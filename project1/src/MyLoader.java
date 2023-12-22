import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URL;
import java.sql.*;
import java.util.ArrayList;
import java.util.Objects;
import java.util.Properties;


public class MyLoader {
    private static final int BATCH_SIZE = 50000;

    private static int cntContainer;
    private static int cntCode_City;
    private static int cntCourier;
    private static int cntItemFI;
    private static int cntItemTI;
    private static int cntItemTT;
    private static int cntShip;


    private static ArrayList<String> listContainer = new ArrayList<>();
    private static ArrayList<String> listCode_City = new ArrayList<>();
    private static ArrayList<String> listCourier = new ArrayList<>();
    private static ArrayList<String> listItemFI = new ArrayList<>();
    private static ArrayList<String> listItemTI = new ArrayList<>();
    private static ArrayList<String> listItemTT = new ArrayList<>();
    private static ArrayList<String> listShip = new ArrayList<>();
    private static ArrayList<String> listString = new ArrayList<>();


    private static URL propertyURL = MyLoader.class
            .getResource("loader.cnf");

    private static Connection con = null;

    private static PreparedStatement stmtContainer = null;
    private static PreparedStatement stmtCode_city = null;
    private static PreparedStatement stmtCourier = null;
    private static PreparedStatement stmtItemFI = null;
    private static PreparedStatement stmtItemTI = null;
    private static PreparedStatement stmtItemTT = null;
    private static PreparedStatement stmtShip = null;


    private static boolean verbose = false;

    private static void openDB(String host, String dbname,
                               String user, String pwd) {
        try {
            //
            Class.forName("org.postgresql.Driver");
        } catch (Exception e) {
            System.err.println("Cannot find the Postgres driver. Check CLASSPATH.");
            System.exit(1);
        }
        String url = "jdbc:postgresql://" + host + "/" + dbname;
        Properties props = new Properties();
        props.setProperty("user", user);
        props.setProperty("password", pwd);
        try {
            con = DriverManager.getConnection(url, props);
            if (verbose) {
                System.out.println("Successfully connected to the database "
                        + dbname + " as " + user);
            }
            con.setAutoCommit(false);
        } catch (SQLException e) {
            System.err.println("Database connection failed");
            System.err.println(e.getMessage());
            System.exit(1);
        }
        try {
            stmtContainer = con.prepareStatement("insert into container(container_code,container_type)"
                    + " values(?,?)");
            stmtCode_city = con.prepareStatement("insert into code_city(code,city) values(?,?)");
            stmtCourier = con.prepareStatement("insert into courier(courier_phone_number,courier_name," +
                    "courier_gender,courier_work_place,company,courier_age,courier_log_time) values(?,?,?,?,?,?,?)");
            stmtItemFI = con.prepareStatement("insert into item_fundamental_information(item_name," +
                    "item_type,item_price,item_export_tax,item_import_tax,item_log_time) values(?,?,?,?,?,?)");
            stmtItemTI = con.prepareStatement("insert into item_transportation_information(item_name," +
                    "retrieval_city_code,delivery_city_code,retrieval_courier_phone_number,delivery_courier_phone_number," +
                    "ship_name,container_code) values(?,?,?,?,?,?,?)");
            stmtShip = con.prepareStatement("insert into ship(ship_name,company) values(?,?)");
            stmtItemTT = con.prepareStatement("insert into item_transportation_time(item_name,retrieval_start_time," +
                    "export_time,import_time,delivery_finish_time) values(?,?,?,?,?)");

        } catch (SQLException e) {
            System.err.println("Insert statement failed");
            System.err.println(e.getMessage());
            closeDB();
            System.exit(1);
        }
    }

    private static void closeDB() {
        if (con != null) {
            try {
                if (stmtContainer != null && stmtItemFI != null & stmtCode_city != null && stmtItemTI != null &&
                        stmtItemTT != null && stmtShip != null & stmtCourier != null) {
                    stmtContainer.close();
                    stmtShip.close();
                    stmtItemTT.close();
                    stmtCode_city.close();
                    stmtCourier.close();
                    stmtItemTI.close();
                    stmtItemFI.close();
                }
                con.close();
                con = null;
            } catch (Exception e) {
                // Forget about it
            }
        }
    }


    private static void loadCourier(String phone_number, String name, String gender,
                                    String place, String company, int age, String log_time)
            throws SQLException {
        if (con != null) {
            if (!listCourier.contains(phone_number)) {
                listCourier.add(phone_number);
                cntCourier++;
                stmtCourier.setString(1, phone_number);
                stmtCourier.setString(2, name);
                stmtCourier.setString(3, gender);
                stmtCourier.setString(4, place);
                stmtCourier.setString(5, company);
                stmtCourier.setInt(6, age);
                stmtCourier.setString(7, log_time);
                stmtCourier.addBatch();
                if (cntCourier % BATCH_SIZE == 0) {
                    stmtCourier.executeBatch();
                    stmtCourier.clearBatch();
                }
            }
        }
    }

    private static void loadShip(String name, String company)
            throws SQLException {
        if (con != null) {
            if (!listShip.contains(name)) {
                listShip.add(name);
                cntShip++;
                stmtShip.setString(1, name);
                stmtShip.setString(2, company);
                stmtShip.addBatch();
                if (cntShip % BATCH_SIZE == 0) {
                    stmtShip.executeBatch();
                    stmtShip.clearBatch();
                }
            }
        }
    }

    private static void loadCode_city(String code, String city)
            throws SQLException {
        if (con != null) {
            if (!listCode_City.contains(code)) {
                listCode_City.add(code);
                cntCode_City++;
                stmtCode_city.setString(1, code);
                stmtCode_city.setString(2, city);
                stmtCode_city.addBatch();
                if (cntCode_City % BATCH_SIZE == 0) {
                    stmtCode_city.executeBatch();
                    stmtCode_city.clearBatch();
                }
            }
        }
    }

    private static void loadItem_transportation_time(String name, String start, String export,
                                                     String Import, String finish)
            throws SQLException {
        if (con != null) {
            if (!listItemTT.contains(name)) {
                listItemTT.add(name);
                cntItemTT++;
                stmtItemTT.setString(1, name);
                stmtItemTT.setString(2, start);
                stmtItemTT.setString(3, export);
                stmtItemTT.setString(4, Import);
                stmtItemTT.setString(5, finish);
                stmtItemTT.addBatch();
                if (cntItemTT % BATCH_SIZE == 0) {
                    stmtItemTT.executeBatch();
                    stmtItemTT.clearBatch();
                }
            }

        }
    }

    private static void loadItem_fundamental_Information(String name, String type, String price,
                                                         String export, String Import, String log_time)
            throws SQLException {
        if (con != null) {
            if (!listItemFI.contains(name)) {
                listItemFI.add(name);
                cntItemFI++;
                stmtItemFI.setString(1, name);
                stmtItemFI.setString(2, type);
                stmtItemFI.setString(3, price);
                stmtItemFI.setString(4, export);
                stmtItemFI.setString(5, Import);
                stmtItemFI.setString(6, log_time);
                stmtItemFI.addBatch();//why different with executeUpdate
                if (cntItemFI % BATCH_SIZE == 0) {
                    stmtItemFI.executeBatch();
                    stmtItemFI.clearBatch();
                }
            }
        }
    }

    private static void loadContainer(String Container_code, String Container_Type)
            throws SQLException {
        if (con != null) {
            if (!listContainer.contains(Container_code)) {
                listContainer.add(Container_code);
                cntContainer++;
                stmtContainer.setString(1, Container_code);
                stmtContainer.setString(2, Container_Type);
                stmtContainer.addBatch();
                if (cntContainer % BATCH_SIZE == 0) {
                    stmtContainer.executeBatch();
                    stmtContainer.clearBatch();
                }
            }
        }
    }

    private static void loadItem_transportation_information(String name, String Rcode, String Dcode, String RPhone,
                                                            String DPhone, String ship_name, String container_code)
            throws SQLException {
        if (con != null) {
            if (!listItemTI.contains(name)) {
                listItemTI.add(name);
                cntItemTI++;
                stmtItemTI.setString(1, name);
                stmtItemTI.setString(2, Rcode);
                stmtItemTI.setString(3, Dcode);
                stmtItemTI.setString(4, RPhone);
                stmtItemTI.setString(5, DPhone);
                stmtItemTI.setString(6, ship_name);
                stmtItemTI.setString(7, container_code);
                stmtItemTI.addBatch();//why different with executeUpdate
                if (cntItemTI % BATCH_SIZE == 0) {
                    stmtItemTI.executeBatch();
                    stmtItemTI.clearBatch();
                }
            }
        }
    }

    private static void executeAndClearPreBatch()
            throws SQLException {

        if (cntContainer % BATCH_SIZE != 0) {
            stmtContainer.executeBatch();
            stmtContainer.clearBatch();
        }
        if (cntCourier % BATCH_SIZE != 0) {
            stmtCourier.executeBatch();
            stmtCourier.clearBatch();
        }
        if (cntCode_City % BATCH_SIZE != 0) {
            stmtCode_city.executeBatch();
            stmtCode_city.clearBatch();
        }
        if (cntItemFI % BATCH_SIZE != 0) {
            stmtItemFI.executeBatch();
            stmtItemFI.clearBatch();
        }
        if (cntItemTT % BATCH_SIZE != 0) {
            stmtItemTT.executeBatch();
            stmtItemTT.clearBatch();
        }
        if (cntShip % BATCH_SIZE != 0) {
            stmtShip.executeBatch();
            stmtShip.clearBatch();
        }
    }

    private static void closePreStmt()
            throws SQLException {
        stmtShip.close();
        stmtCourier.close();
        stmtItemFI.close();
        stmtItemTT.close();
        stmtItemTI.close();
        stmtContainer.close();
        stmtCode_city.close();
    }

    public static void main(String[] args) {
        String fileName = null;
        boolean verbose = false;

        switch (args.length) {
            case 1 -> fileName = args[0];
            case 2 -> {
                if ("-v".equals(args[0])) {
                    verbose = true;
                } else {
                    System.err.println("Usage: java [-v] GoodLoader filename");
                    System.exit(1);
                }
                fileName = args[1];
            }
            default -> {
                System.err.println("Usage: java [-v] GoodLoader filename");
                System.exit(1);
            }
        }

        if (propertyURL == null) {
            System.err.println("No configuration file (loader.cnf) found");
            System.exit(1);
        }

        Properties defprop = new Properties();
        defprop.put("host", "localhost");
        defprop.put("user", "checker");
        defprop.put("password", "123456");
        defprop.put("database", "Shipment_records");
        Properties prop = new Properties(defprop);
        try (BufferedReader conf
                     = new BufferedReader(new FileReader(propertyURL.getPath()))) {
            prop.load(conf);
        } catch (IOException e) {
            // Ignore
            System.err.println("No configuration file (loader.cnf) found");
        }
        try (BufferedReader infile
                     = new BufferedReader(new FileReader(fileName))) {
            long start = 0;
            long end = 0;
            String line = null;
            String[] parts = null;
            String Item_type, Item_name, Recity, Rsstart, Rename, Regender, RePhoneNumber, Definish, Dename, Decity, Degender,
                    DePhoneNumber, export_city, import_city, export_time, import_time, container_name, container_type, ship_name, company, log_time, price, ImportTax, ExportTax;
            ;

            int Reage, Deage;
            int cnt = 0;
            // Empty target table
            openDB(prop.getProperty("host"), prop.getProperty("database"), prop.getProperty("user"), prop.getProperty("password"));
            Statement stmt0;
            if (con != null) {
                stmt0 = con.createStatement();
                stmt0.execute("truncate table container,code_city,courier,item_fundamental_information," +
                        "item_transportation_information,item_transportation_time,ship");
                con.commit();
                stmt0.close();


            }
            closeDB();

            //
            start = System.currentTimeMillis();
            openDB(prop.getProperty("host"), prop.getProperty("database"),
                    prop.getProperty("user"), prop.getProperty("password"));
            while ((line = infile.readLine()) != null) {
                if (cnt == 0) {
                    cnt++;
                    continue;
                }
                listString.add(line);
                parts = line.split(",");
                if (parts.length > 1) {
                    Item_name = parts[0];
                    Item_type = parts[1];
                    price = parts[2];
                    Recity = parts[3];
                    Rsstart = parts[4];
                    Rename = parts[5];
                    Regender = parts[6];
                    RePhoneNumber = parts[7];
                    Reage = (int) Double.parseDouble(parts[8]);
                    Definish = parts[9];
                    Decity = parts[10];
                    Dename = parts[11];
                    Degender = parts[12];
                    DePhoneNumber = parts[13];
                    if (parts[14].equals("")) {
                        Deage = 0;
                    } else {
                        Deage = (int) Double.parseDouble(parts[14]);
                    }
                    export_city = parts[15];
                    ExportTax = parts[16];
                    export_time = parts[17];
                    import_city = parts[18];
                    ImportTax = parts[19];
                    import_time = parts[20];
                    container_name = parts[21];
                    container_type = parts[22];
                    ship_name = parts[23];
                    company = parts[24];
                    log_time = parts[25];
                    String RePhone;
                    String DePhone;
                    String ReCode;
                    String DeCode;
                    if (!Objects.equals(RePhoneNumber, "")) {
                        RePhone = RePhoneNumber.substring(5);
                        ReCode = RePhoneNumber.substring(0, 4);
                    } else {
                        RePhone = "";
                        ReCode = "";
                    }
                    if (!Objects.equals(DePhoneNumber, "")) {
                        DePhone = DePhoneNumber.substring(5);
                        DeCode = DePhoneNumber.substring(0, 4);
                    } else {
                        DePhone = "";
                        DeCode = "";
                    }

                    loadCode_city(ReCode, Recity);
                    loadCode_city(DeCode, Decity);
                    loadCourier(RePhone, Rename, Regender, import_city, company, Reage, log_time);
                    loadCourier(DePhone, Dename, Degender, export_city, company, Deage, log_time);
                    loadContainer(container_name, container_type);
                    loadShip(ship_name, company);
                    loadItem_fundamental_Information(Item_name, Item_type, price, ExportTax, ImportTax, log_time);
                    loadItem_transportation_time(Item_name, Rsstart, export_time, import_time, Definish);


                      cnt++;
//                    if (cnt == 5000) {
//                        break;
//                    }

                }


            }
            executeAndClearPreBatch();
            for (String s : listString) {
                parts = s.split(",");
                if (parts.length > 1) {
                    Item_name = parts[0];
                    RePhoneNumber = parts[7];
                    DePhoneNumber = parts[13];
                    container_name = parts[21];
                    ship_name = parts[23];
                    String RePhone;
                    String DePhone;
                    String ReCode;
                    String DeCode;
                    if (!Objects.equals(RePhoneNumber, "")) {
                        RePhone = RePhoneNumber.substring(5);
                        ReCode = RePhoneNumber.substring(0, 4);
                    } else {
                        RePhone = "";
                        ReCode = "";
                    }
                    if (!Objects.equals(DePhoneNumber, "")) {
                        DePhone = DePhoneNumber.substring(5);
                        DeCode = DePhoneNumber.substring(0, 4);
                    } else {
                        DePhone = "";
                        DeCode = "";
                    }


                    loadItem_transportation_information(Item_name, ReCode, DeCode
                            , RePhone, DePhone, ship_name, container_name);
                }
            }

            if (cntItemTI % BATCH_SIZE != 0) {
                stmtItemTI.executeBatch();
                stmtItemTI.clearBatch();
            }

            con.commit();
            closePreStmt();
            closeDB();
            end = System.currentTimeMillis();
            System.out.println(cnt + " records successfully loaded");
            System.out.println("Loading speed : "
                    + ((cnt) * 1000L) / (end - start)
                    + " records/s");
            System.out.println("Loading time : " + (end - start) + " ms");
        } catch (SQLException se) {
            System.err.println("SQL error: " + se.getMessage());
            try {
                con.rollback();
                stmtContainer.close();
            } catch (Exception e2) {
            }
            closeDB();
            System.exit(1);
        } catch (IOException e) {
            System.err.println("Fatal error: " + e.getMessage());
            try {
                con.rollback();
                stmtContainer.close();
            } catch (Exception e2) {
            }
            closeDB();
            System.exit(1);
        }
        closeDB();
    }
}
