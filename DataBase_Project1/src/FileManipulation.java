import java.io.*;
import java.util.*;

public class FileManipulation implements DataManipulation {

    public String deleteOneItem(String x){
        StringBuilder sb=new StringBuilder();
        try (BufferedReader bufferedReader = new BufferedReader(new FileReader("item_transportation_information1.csv"))) {
            String line;
            String id;
            while ((line = bufferedReader.readLine()) != null) {
                id=line.split(",")[2];

                if(!Objects.equals(id, x)) sb.append(line+"\n");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        try (FileWriter writer = new FileWriter("item_transportation_information1.csv")) {
            writer.write(sb.toString());
        } catch (IOException e) {
            e.printStackTrace();

        }
        return x;}
    public int updateDeCode(String str2, String x){
        StringBuilder sb=new StringBuilder();
        try (BufferedReader bufferedReader = new BufferedReader(new FileReader("item_transportation_information1.csv"))) {

            String line;
            String id;
            while ((line = bufferedReader.readLine()) != null) {
                id=line.split(",")[0];

                if(!Objects.equals(id, x)) sb.append(line+"\n");
                else {
                    StringBuilder up=new StringBuilder();
                    String[] update=line.split(",");
                    update[2]=str2;
                    for(int i=0;i<update.length;i++){
                        up.append(update[i]);
                        if(i<update.length-1) up.append(",");
                        else up.append("\n");
                    }
                    sb.append(up);

                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        try (FileWriter writer = new FileWriter("item_transportation_information1.csv")) {
            writer.write(sb.toString());
        } catch (IOException e) {
            e.printStackTrace();

        }
        return 1;}
    public int updateReCode(String str2, String x){
        StringBuilder sb=new StringBuilder();
        try (BufferedReader bufferedReader = new BufferedReader(new FileReader("item_transportation_information1.csv"))) {

            String line;
            String id;
            while ((line = bufferedReader.readLine()) != null) {
                id=line.split(",")[0];

                if(!Objects.equals(id, x)) sb.append(line+"\n");
                else {
                    StringBuilder up=new StringBuilder();
                    String[] update=line.split(",");
                    for(int i=0;i<update.length;i++){
                        if(i!=1) up.append(update[i]);
                        else up.append(str2);
                        if(i<update.length-1) up.append(",");
                        else up.append("\n");
                    }
                    sb.append(up);

                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        try (FileWriter writer = new FileWriter("item_transportation_information1.csv")) {
            writer.write(sb.toString());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return 1;}
    @Override
    public int addOneItem(String str) {
        try (FileWriter writer = new FileWriter("item_transportation_information1.csv",true)) {
            writer.write(str+"\n");

        } catch (IOException e) {
            e.printStackTrace();
            return 0;
        }
        return 1;
    }

    @Override
    public String select_ReCode() {
        String line;
        int start_time = 1;
        Set<String> timeMap = new HashSet<>();
        StringBuilder sb = new StringBuilder();

        try (BufferedReader bufferedReader = new BufferedReader(new FileReader("item_transportation_information1.csv"))) {
            while ((line = bufferedReader.readLine()) != null) {
                line = line.split(",")[start_time];
                if (!timeMap.contains(line)) {
                    sb.append(line).append("\n");
                    timeMap.add(line);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return sb.toString();
    }

    @Override
    public String Select_ReCode_count() {
        String line;
        Map<String, Integer> continentCount = new HashMap<>();
        StringBuilder sb = new StringBuilder();
        try (BufferedReader bufferedReader = new BufferedReader(new FileReader("item_transportation_information1.csv"))) {
            while ((line = bufferedReader.readLine()) != null) {
                line = line.split(",")[1];
                if (continentCount.containsKey(line)) {
                    continentCount.put(line, continentCount.get(line) + 1);
                } else {
                    continentCount.put(line, 1);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        for (Map.Entry<String, Integer> entry : continentCount.entrySet()) {
            sb.append(entry.getKey())
                    .append("\t");
//                    .append(entry.getValue())
//                    .append("\n");
        }

        return sb.toString();
    }

    @Override
    public String Select_by_ReCode() {
        String line;
        Map<String, Integer> continentCount = new HashMap<>();
        StringBuilder sb = new StringBuilder();
        try (BufferedReader bufferedReader = new BufferedReader(new FileReader("item_transportation_information1.csv"))) {
            while ((line = bufferedReader.readLine()) != null) {
                String line1 = line.split(",")[1];
                if (line1.equals("1084")) {
                    continentCount.put(line,  1);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        for (Map.Entry<String, Integer> entry : continentCount.entrySet()) {
            sb.append(entry.getKey())
                    .append("\t");
//                    .append(entry.getValue())
//                    .append("\n");
        }

        return sb.toString();
    }










    public void getTxt(){}


}
