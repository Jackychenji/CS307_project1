
public class Client {

    public static void main(String[] args) {
        try {
            DataManipulation dm = new DataFactory().createDataManipulation(args[0]);
            System.out.println(args[0]);
            dm.getTxt();
//insert
            long start1 = System.currentTimeMillis();
            for (int i = 0; i < 1000; i++) {
                dm.addOneItem("mango-"+i+",3629,5291,1675146020,1447953920,·ü²¨ºÅ,79ea685f");
            }

            long end1 = System.currentTimeMillis();
            System.out.println("insert 1000 items cost " + (end1 - start1) + "ms");

//update
            long start2 = System.currentTimeMillis();
            for (int i = 0; i < 1000; i++) {
                String mango = "mango-"+i ;
                dm.updateReCode("3629", mango);
            }
            long end2 = System.currentTimeMillis();
            System.out.println("update 1000 Retrieval Code cost " + (end2 - start2) + "ms");

//select
            long start30 = System.currentTimeMillis();//1
            dm.select_ReCode();
            long end30 = System.currentTimeMillis();
            System.out.println("select Retrieval Code cost " + (end30 - start30) + "ms");


            long start31 = System.currentTimeMillis();//2
            dm.Select_by_ReCode();
            long end31 = System.currentTimeMillis();
            System.out.println("Select retrieval_city_code = 1084 cost " + (end31 - start31) + "ms");

            long start32 = System.currentTimeMillis();//3
            dm.Select_ReCode_count();
            long end32 = System.currentTimeMillis();
            System.out.println("select count of Retrieval code " + (end32 - start32) + "ms");



//delete
            long start4 = System.currentTimeMillis();

                dm.deleteOneItem("3629");

            long end4 = System.currentTimeMillis();
            System.out.println("delete all delivery_city_code = 5291 cost " + (end4 - start4) + "ms");
        } catch (IllegalArgumentException e) {
            System.err.println(e.getMessage());
        }
    }
}

