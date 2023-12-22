import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URL;
import java.sql.*;
import java.util.*;


public class BetterLoader {
    private static final int BATCH_SIZE = 5000;

    private static int cntContainer;
    private static int cntCode_City;
    private static int cntCourier;
    private static int cntItemFI;
    private static int cntItemTI;
    private static int cntItemTT;
    private static int cntShip;

    static class Container {
        String container_code;
        String container_type;

        public Container(String container_code, String container_type) {
            this.container_code = container_code;
            this.container_type = container_type;
        }
    }

    static class Code_City {
        String Code;
        String City;

        public Code_City(String code, String city) {
            Code = code;
            City = city;
        }
    }

    static class Courier {
        String courier_phone_number;
        String courier_name;
        String courier_gender;
        String courier_work_place;
        String company;
        int courier_age;
        String courier_log_time;

        public Courier(String courier_phone_number, String courier_name, String courier_gender, String courier_work_place, String company, int courier_age, String courier_log_time) {
            this.courier_phone_number = courier_phone_number;
            this.courier_name = courier_name;
            this.courier_gender = courier_gender;
            this.courier_work_place = courier_work_place;
            this.company = company;
            this.courier_age = courier_age;
            this.courier_log_time = courier_log_time;
        }
    }

    static class ItemFI {
        String item_name;
        String item_type;
        String item_price;
        String item_export_tax;
        String item_import_tax;
        String item_log_time;

        public ItemFI(String item_name, String item_type, String item_price, String item_export_tax, String item_import_tax, String item_log_time) {
            this.item_name = item_name;
            this.item_type = item_type;
            this.item_price = item_price;
            this.item_export_tax = item_export_tax;
            this.item_import_tax = item_import_tax;
            this.item_log_time = item_log_time;
        }
    }

    static class ItemTI {
        String item_name;
        String delivery_city_code;
        String retrieval_city_code;
        String delivery_courier_phone_number;
        String retrieval_courier_phone_number;
        String ship_name;
        String container_code;


        public ItemTI(String item_name, String delivery_city_code, String retrieval_city_code, String delivery_courier_phone_number, String retrieval_courier_phone_number, String ship_name, String container_code) {
            this.item_name = item_name;
            this.delivery_city_code = delivery_city_code;
            this.retrieval_city_code = retrieval_city_code;
            this.delivery_courier_phone_number = delivery_courier_phone_number;
            this.retrieval_courier_phone_number = retrieval_courier_phone_number;
            this.ship_name = ship_name;
            this.container_code = container_code;
        }
    }

    static class ItemTT {
        String item_name;
        String retrieval_start_time;
        String export_time;
        String import_time;
        String delivery_finish_time;

        public ItemTT(String item_name, String retrieval_start_time, String export_time, String import_time, String delivery_finish_time) {
            this.item_name = item_name;
            this.retrieval_start_time = retrieval_start_time;
            this.export_time = export_time;
            this.import_time = import_time;
            this.delivery_finish_time = delivery_finish_time;
        }
    }

    static class Ship {
        String ship_name;
        String company;

        public Ship(String ship_name, String company) {
            this.ship_name = ship_name;
            this.company = company;
        }
    }


    private static final Map<String, Container> ContainerMap = new HashMap<>();
    private static final Map<String, Code_City> Code_CityMap = new HashMap<>();
    private static final Map<String, Courier> CourierMap = new HashMap<>();
    private static final Map<String, ItemFI> ItemFIMap = new HashMap<>();
    private static final Map<String, ItemTI> ItemTIMap = new HashMap<>();
    private static final Map<String, ItemTT> ItemTTMap = new HashMap<>();
    private static final Map<String, Ship> ShipMap = new HashMap<>();


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


    private static void loadCourier()
            throws SQLException {
        if (con != null) {
            int i = 0;
            Courier courier;
            for (Map.Entry<String, Courier> ignored : CourierMap.entrySet()) {
                courier = ignored.getValue();
                i++;
                stmtCourier.setString(1, courier.courier_phone_number);
                stmtCourier.setString(2, courier.courier_name);
                stmtCourier.setString(3, courier.courier_gender);
                stmtCourier.setString(4, courier.courier_work_place);
                stmtCourier.setString(5, courier.company);
                stmtCourier.setInt(6, courier.courier_age);
                stmtCourier.setString(7, courier.courier_log_time);
                stmtCourier.addBatch();
                if (i % BATCH_SIZE == 0) {
                    stmtCourier.executeBatch();
                    stmtCourier.clearBatch();
                }
            }
            if (CourierMap.size() % BATCH_SIZE != 0) {
                stmtCourier.executeBatch();
                stmtCourier.clearBatch();
            }
        }
    }


    private static void loadShip()
            throws SQLException {
        if (con != null) {
            int i = 0;
            Ship ship;
            for (Map.Entry<String, Ship> ignored : ShipMap.entrySet()) {
                ship = ignored.getValue();
                i++;
                stmtShip.setString(1, ship.ship_name);
                stmtShip.setString(2, ship.company);

                stmtShip.addBatch();
                if (i % BATCH_SIZE == 0) {
                    stmtShip.executeBatch();
                    stmtShip.clearBatch();
                }
            }
            if (ShipMap.size() % BATCH_SIZE != 0) {
                stmtShip.executeBatch();
                stmtShip.clearBatch();
            }
        }
    }

    private static void loadCode_city()
            throws SQLException {
        if (con != null) {
            int i = 0;
            Code_City code_city;
            for (Map.Entry<String, Code_City> ignored : Code_CityMap.entrySet()) {
                code_city = ignored.getValue();
                i++;
                stmtCode_city.setString(1, code_city.Code);
                stmtCode_city.setString(2, code_city.City);;
                stmtCode_city.addBatch();
                if (i % BATCH_SIZE == 0) {
                    stmtCode_city.executeBatch();
                    stmtCode_city.clearBatch();
                }
            }
            if (Code_CityMap.size() % BATCH_SIZE != 0) {
                stmtCode_city.executeBatch();
                stmtCode_city.clearBatch();
            }
        }
    }

    private static void loadItem_transportation_time()
            throws SQLException {
        if (con != null) {
            int i = 0;
            ItemTT itemTT;
            for (Map.Entry<String, ItemTT> ignored : ItemTTMap.entrySet()) {
                itemTT = ignored.getValue();
                i++;
                stmtItemTT.setString(1, itemTT.item_name);
                stmtItemTT.setString(2, itemTT.retrieval_start_time);
                stmtItemTT.setString(3, itemTT.export_time);
                stmtItemTT.setString(4, itemTT.import_time);
                stmtItemTT.setString(5, itemTT.delivery_finish_time);
                stmtItemTT.addBatch();
                if (i % BATCH_SIZE == 0) {
                    stmtItemTT.executeBatch();
                    stmtItemTT.clearBatch();
                }
            }
            if (CourierMap.size() % BATCH_SIZE != 0) {
                stmtItemTT.executeBatch();
                stmtItemTT.clearBatch();
            }
        }
    }

    private static void loadItem_fundamental_Information()
            throws SQLException {
        if (con != null) {
            int i = 0;
            ItemFI itemFI;
            for (Map.Entry<String, ItemFI> ignored : ItemFIMap.entrySet()) {
                itemFI = ignored.getValue();
                i++;
                stmtItemFI.setString(1, itemFI.item_name);
                stmtItemFI.setString(2, itemFI.item_type);
                stmtItemFI.setString(3, itemFI.item_price);
                stmtItemFI.setString(4, itemFI.item_export_tax);
                stmtItemFI.setString(5, itemFI.item_import_tax);
                stmtItemFI.setString(6, itemFI.item_log_time);
                stmtItemFI.addBatch();
                if (i % BATCH_SIZE == 0) {
                    stmtItemFI.executeBatch();
                    stmtItemFI.clearBatch();
                }
            }
            if (ItemFIMap.size() % BATCH_SIZE != 0) {
                stmtItemFI.executeBatch();
                stmtItemFI.clearBatch();
            }
        }
    }

    private static void loadContainer()
            throws SQLException {
        if (con != null) {
            int i = 0;
            Container container;
            for (Map.Entry<String, Container> ignored : ContainerMap.entrySet()) {
                container = ignored.getValue();
                i++;
                stmtContainer.setString(1, container.container_code);
                stmtContainer.setString(2, container.container_type);;
                stmtContainer.addBatch();
                if (i % BATCH_SIZE == 0) {
                    stmtContainer.executeBatch();
                    stmtContainer.clearBatch();
                }
            }
            if (ContainerMap.size() % BATCH_SIZE != 0) {
                stmtContainer.executeBatch();
                stmtContainer.clearBatch();
            }
        }
    }

    private static void loadItem_transportation_information()
            throws SQLException {
        if (con != null) {
            int i = 0;
            ItemTI itemTI;
            for (Map.Entry<String, ItemTI> ignored : ItemTIMap.entrySet()) {
                itemTI = ignored.getValue();
                i++;
                stmtItemTI.setString(1, itemTI.item_name);
                stmtItemTI.setString(2, itemTI.retrieval_city_code);
                stmtItemTI.setString(3, itemTI.delivery_city_code);
                stmtItemTI.setString(4, itemTI.retrieval_courier_phone_number);
                stmtItemTI.setString(5, itemTI.delivery_courier_phone_number);
                stmtItemTI.setString(6, itemTI.ship_name);
                stmtItemTI.setString(7, itemTI.container_code);
                stmtItemTI.addBatch();//why different with executeUpdate
                if (i % BATCH_SIZE == 0) {
                    stmtItemTI.executeBatch();
                    stmtItemTI.clearBatch();
                }
            }
            if (ItemTIMap.size() % BATCH_SIZE != 0) {
                stmtItemTI.executeBatch();
                stmtItemTI.clearBatch();
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
        stmtContainer.close();
        stmtCode_city.close();
    }

    public static void main(String[] args) {
        String fileName = null;
        boolean verbose = false;

        switch (args.length) {
            case 1:
                fileName = args[0];
                break;
            case 2:
                switch (args[0]) {
                    case "-v":
                        verbose = true;
                        break;
                    default:
                        System.err.println("Usage: java [-v] GoodLoader filename");
                        System.exit(1);
                }
                fileName = args[1];
                break;
            default:
                System.err.println("Usage: java [-v] GoodLoader filename");
                System.exit(1);
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
            long start;
            long end;
            String line;
            String[] parts;
            String Item_type, Item_name, Recity, Rsstart, Rename, Regender, RePhoneNumber, Definish, Dename, Decity, Degender,
                    DePhoneNumber, export_city, import_city, export_time, import_time, container_name, container_type, ship_name, company, log_time, price, ImportTax, ExportTax;
            ;

            int Reage, Deage;
            int cnt = 0;
            start = System.currentTimeMillis();

            while ((line = infile.readLine()) != null) {
                if (cnt == 0) {
                    cnt++;
                    continue;
                }
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

                    if (ReCode.equals("")){
                        Recity = "";
                    }
                    if (DeCode.equals("")){
                        Decity = "";
                    }

                    Code_City code_city = new Code_City(ReCode,Recity);
                    Code_City code_city1 = new Code_City(DeCode,Decity);
                    Courier courier = new Courier(RePhone, Rename, Regender, export_city, company, Reage, log_time);
                    Courier courier1 = new Courier(DePhone, Dename, Degender, import_city, company, Deage, log_time);
                    Container container = new Container(container_name, container_type);
                    Ship ship = new Ship(ship_name, company);
                    ItemFI itemFI= new ItemFI(Item_name, Item_type, price, ExportTax, ImportTax, log_time);
                    ItemTT itemTT = new ItemTT(Item_name, Rsstart, export_time, import_time, Definish);
                    ItemTI itemTI = new ItemTI(Item_name, ReCode, DeCode, RePhone, DePhone, ship_name, container_name);


                    Code_CityMap.put(ReCode,code_city);
                    Code_CityMap.put(DeCode,code_city1);
                    CourierMap.put(RePhone,courier);
                    CourierMap.put(DePhone,courier1);
                    ContainerMap.put(container_name,container);
                    ShipMap.put(ship_name,ship);
                    ItemFIMap.put(Item_name,itemFI);
                    ItemTTMap.put(Item_name,itemTT);
                    ItemTIMap.put(Item_name,itemTI);


                    cnt++;
//                    if (cnt == 50000) {
//                        break;
//                    }

                }


            }
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

            openDB(prop.getProperty("host"), prop.getProperty("database"),
                    prop.getProperty("user"), prop.getProperty("password"));







            long sum_start = System.currentTimeMillis();
            start = System.currentTimeMillis();
            loadContainer();
            end = System.currentTimeMillis();
//            System.out.println(ContainerMap.size() + " records successfully loaded in Container");
//            System.out.println("Loading speed : "
//                    + (ContainerMap.size() * 1000L) / (end - start)
//                    + " records/s");

            start = System.currentTimeMillis();
            loadCode_city();
            end = System.currentTimeMillis();
//            System.out.println(Code_CityMap.size() + " records successfully loaded in Code_City");
//            System.out.println("Loading speed : "
//                    + (Code_CityMap.size() * 1000L) / (end - start)
//                    + " records/s");

            start = System.currentTimeMillis();
            loadCourier();
            end = System.currentTimeMillis();
//            System.out.println(CourierMap.size() + " records successfully loaded in Courier");
//            System.out.println("Loading speed : "
//                    + (CourierMap.size() * 1000L) / (end - start)
//                    + " records/s");

            start = System.currentTimeMillis();
            loadItem_transportation_time();
            end = System.currentTimeMillis();
//            System.out.println(ItemTTMap.size() + " records successfully loaded in item_transportation_time");
//            System.out.println("Loading speed : "
//                    + (ItemTTMap.size() * 1000L) / (end - start)
//                    + " records/s");

            start = System.currentTimeMillis();
            loadShip();
            end = System.currentTimeMillis();
//            System.out.println(ShipMap.size() + " records successfully loaded in Ship");
//            System.out.println("Loading speed : "
//                    + (ShipMap.size() * 1000L) / (end - start)
//                    + " records/s");

            start = System.currentTimeMillis();
            loadItem_fundamental_Information();
            end = System.currentTimeMillis();
//            System.out.println(ItemFIMap.size() + " records successfully loaded in item_fundamental_information");
//            System.out.println("Loading speed : "
//                    + (ItemFIMap.size() * 1000L) / (end - start)
//                    + " records/s");

            con.commit();
            closePreStmt();


            start = System.currentTimeMillis();
            loadItem_transportation_information();
            con.commit();
            stmtItemTI.close();
            end = System.currentTimeMillis();
//            System.out.println(ItemTIMap.size() + " records successfully loaded in item_transportation_information");
//            System.out.println("Loading speed : "
//                    + (ItemTIMap.size() * 1000L) / (end - start)
//                    + " records/s");

            long end_sum = System.currentTimeMillis();
            long sum = ItemTTMap.size()+ItemTIMap.size()+ItemFIMap.size()+ShipMap.size()+CourierMap.size()
                    +ContainerMap.size()+Code_CityMap.size();
            System.out.println(cnt + " records successfully loaded");
            System.out.println("Loading speed : "
                    + (cnt * 1000L) / (end_sum - sum_start)
                    + " records/s");
            System.out.println("Loading time : "
                    +  (end_sum - sum_start)
                    + " ms");
            closeDB();


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
