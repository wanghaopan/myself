import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.security.UserGroupInformation;

public class Demo {
    public static void main(String[] args) throws IOException {
        // System.out.println("args[0]=>" + args[0]);

        Configuration conf = new Configuration();

        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");

        conf.addResource(new Path("/home/pengxh1/etc/hadoop/conf/core-site.xml"));

        conf.addResource(new Path("/home/pengxh1/etc/hadoop/conf/hdfs-site.xml"));

        System.setProperty("java.security.krb5.conf",
                "/home/pengxh1/krb5.conf");

        try {
            UserGroupInformation.setConfiguration(conf);
            UserGroupInformation.loginUserFromKeytab("pengxh1@HQ0001.TDH",
                    "/home/pengxh1/pengxh1.keytab");

            System.out.println("kinit success");
            System.out.println();
            FileSystem fs = FileSystem.get(conf);
            System.out.println(conf.toString());
            System.out.println();
            System.out.println(fs.getUri());
            System.out.println();
            System.out.println(fs);
            listDataFile(fs, "hdfs://service/out/", 5);

        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static void listDataFile(FileSystem fs, String hdfsPath, int num) throws IOException {

        RemoteIterator<LocatedFileStatus> iterListFiles = fs.listFiles(new Path(hdfsPath), true);
        int i = 0;
        while (iterListFiles.hasNext() && i < num) {
            LocatedFileStatus locatedFileStatus = iterListFiles.next();

            Path path = locatedFileStatus.getPath();
            System.out.println("path:" + path.toString());
            i++;
        }
    }
}
