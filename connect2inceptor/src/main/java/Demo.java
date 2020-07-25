import java.sql.*;
import java.util.Calendar;
import java.util.Date;

public class Demo {
    private static String driverName = "org.apache.hive.jdbc.HiveDriver";
    public static void main(String[] args) throws  SQLException {
        Calendar start_cal = Calendar.getInstance();
        start_cal.setTime(new Date());
        //获取开始时间戳
        long startTime = start_cal.getTimeInMillis()/1000;
        System.out.println("开始时间："+startTime);
        try {
            Class.forName(driverName);

        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        }
        //Hive2 JDBC URL with LDAP
        String jdbcURL = "jdbc:hive2://10.168.25.189:31050/default";
        String user = "demo";
        String password = "demopasswd";
        System.out.println("----------------------准备链接--------");
        Connection conn = DriverManager.getConnection(jdbcURL, user, password);
        Statement stmt = conn.createStatement();
        //获取数据集
        ResultSet rs = stmt.executeQuery("select * from test.dwd_cus_order_usernum_4g_010_ext_test11 limit 10");

        ResultSetMetaData rsmd = rs.getMetaData();
        int size = rsmd.getColumnCount();
        while(rs.next()) {
            StringBuffer value = new StringBuffer();
            for(int i = 0; i < size; i++) {
                value.append(rs.getString(i+1)).append("\t");
            }
            System.out.println(value.toString());
        }
//关闭连接，释放资源
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