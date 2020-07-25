import java.io.IOException;
import java.sql.*;
import java.util.Calendar;
import java.util.Date;

public class Test {




    //Hive2 Driver class name
    private static String driverName = "org.apache.hive.jdbc.HiveDriver";
    public static void main(String[] args) throws SQLException, IOException {

        Calendar start_cal = Calendar.getInstance();
        start_cal.setTime(new Date());
        //获取开始时间戳
        long startTime = start_cal.getTimeInMillis()/1000;

        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        }
        //使用该连接方式时多一个依赖guava-14.0.1.jar
        String jdbcURL = "jdbc:hive2://10.168.25.189:31050/default;guardianToken=XXXXXXXXDEMO.TDH";
        Connection conn = DriverManager.getConnection(jdbcURL);
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT * FROM test.es0311;");
        ResultSetMetaData rsmd = rs.getMetaData();
        int size = rsmd.getColumnCount();
        while(rs.next()) {
            StringBuffer value = new StringBuffer();
            for(int i = 0; i < size; i++) {
                value.append(rs.getString(i+1)).append("\t");
            }
            System.out.println(value.toString());
        }
        rs.close();
        stmt.close();
        conn.close();

        Calendar end_cal = Calendar.getInstance();
        end_cal.setTime(new Date());
        //获取当前时间戳
        long endTime = end_cal.getTimeInMillis()/1000;

        System.out.print("程序运行时间：");
        System.out.println(endTime-startTime);
    }




}
