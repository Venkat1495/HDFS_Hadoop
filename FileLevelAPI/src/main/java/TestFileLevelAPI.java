import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.client.HdfsDataInputStream;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfoWithStorage;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;

import java.io.*;
import java.util.HashSet;
import java.util.List;
import java.util.ArrayList;
import java.util.Set;

public class TestFileLevelAPI {

    public static Set<String> set = new HashSet<String>();
    public static final Log log = LogFactory.getLog(TestFileLevelAPI.class);
    public static String Owner_Name = null;
    public static int l = 0;
    public static long File_Len = 0;
    public static List<String> fileList = new ArrayList<String>();
    public static List<List<Long>> len = new ArrayList<List<Long>>();

    public void createFile(String fName,String Owner_Name,long File_Len) throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path(fName);
        FSDataOutputStream out = fs.create(path);
//        if (i == 0) {
//            out = fs.create(path);
//            log.info("test create" + i);
//            i = i + 10;
//        }
//        else {
//            out = fs.append(path);
//            log.info("test append");
//        }
        //
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(out));
        bw.newLine();
        bw.write("Owner of the file : " + Owner_Name);
        bw.newLine();
        bw.write("File Len : " + File_Len);
        bw.newLine();
//        for (int i=0; i<15; i++) {
//            bw.write("New Record " + i);
//            bw.newLine();
//        }
        bw.close();
    }

    public void readFile(String fName) throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path(fName);
        FSDataInputStream in = fs.open(path);
        //
        BufferedReader br = new BufferedReader(new InputStreamReader(in));
        String line = null;
        while ((line = br.readLine()) != null) {
            log.info(line);
        }
        in.close();
    }

    public void getBlockInfo(String fName) throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path(fName);
        HdfsDataInputStream hdin = (HdfsDataInputStream)fs.open(path);
        List<LocatedBlock> lblks = hdin.getAllBlocks();
        log.info("Number of blocks: " + lblks.size());
        for (LocatedBlock lblk : lblks) {
            ExtendedBlock eb = lblk.getBlock();
            Block lb = eb.getLocalBlock();
            log.info("Block pool id : " + eb.getBlockPoolId());
            log.info("Block Id : " + eb.getBlockId());
            log.info("Block Name : " + eb.getBlockName());
            log.info("Start Offset of " + lblk.getStartOffset());
            DatanodeInfoWithStorage[] datainfos = lblk.getLocations();
            for (DatanodeInfoWithStorage dn : datainfos) {
                log.info("Data Node Name : " + dn.getHostName());
            }
        }
        //
        FileStatus fStatus = fs.getFileStatus(path);
        log.info("Owner of the file : " + fStatus.getOwner());
        Owner_Name = fStatus.getOwner();
        File_Len = fStatus.getLen();
        log.info("File Len : " + fStatus.getLen());
        log.info("File Block Size : " + fStatus.getBlockSize());
        new TestFileLevelAPI().user_storage_logic(Owner_Name, File_Len);

    }

    public void user_storage_logic(String Owner_Name, long File_len) throws Exception {
        if (l == 0){
            Owner_Name = Owner_Name + "Test";
            l += 10;
        }
        set.add(Owner_Name.trim());
        log.info(set.size());
        for (int i = 0; i < set.size(); i++)  {
            if (len.size() < set.size()){
                len.add(new ArrayList<Long>());
            }
        }
        int m = 0;

        for (String tol : set){

            if (tol == Owner_Name.trim()){
                len.get(m).add(File_len);
            }
            m += 1;
        }


//        if (set.size() == 1) {
//            for (int i = 0; i < set.size(); i++)  {
//                if (len.size() < set.size()){
//                    len.add(new ArrayList<Long>());
//                }
//            }
//            len.get(0).add(File_len);
//        } else if (set.size() > 1) {
//        for (int i = 0; i < set.size(); i++)  {
//            if (len.size() < set.size()){
//                len.add(new ArrayList<Long>());
//            }
//        }
//       String[] names = set.toArray(new String[set.size()]);
//       for(int k = (names.length - 1); k >= 0; k--){
//           log.info("names length " + k + names);
//           if (names[k] == Owner_Name){
//               len.get(k).add(File_len);
//           }
//       }


    }


    public static List<String> getAllFilePath(String filePath) throws Exception {
        Configuration conf = new Configuration();
        FileSystem fsn = FileSystem.get(conf);
        Path path = new Path(filePath);
        String pathOnHadoop = filePath;
//        if (filePath.equalsIgnoreCase("/"))
//            pathOnHadoop = fsn.getWorkingDirectory() + "/";
//        //
        FileStatus[] fileStatus = fsn.listStatus(new Path(pathOnHadoop));
        for (FileStatus fileStat : fileStatus) {
            if (fileStat.isDirectory()) {
                String cpath = String.valueOf(fileStat.getPath());
                getAllFilePath(cpath);
            } else {
                fileList.add(fileStat.getPath().toString());
            }
        }
        for (String file : fileList) {
            new TestFileLevelAPI().getBlockInfo(file);

        }
        new TestFileLevelAPI().createFile("/final_info-2.txt", Owner_Name, File_Len);
        log.info("All the paths available :" + fileList);
        log.info(set.size());
        log.info(set);
        log.info(len);
        return fileList;
    }

    public static void main(String[] args) throws Exception {
        //new TestFileLevelAPI().createFile(args[0]);
        //new TestFileLevelAPI().getBlockInfo(args[0], args[1]);
        //new TestFileLevelAPI().readFile("Repos/HDFS_Hadoop/FileLevelAPI/file-02");
        getAllFilePath(args[0]);

    }

}
