import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.client.HdfsDataInputStream;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfoWithStorage;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.List;

public class TestFileLevelAPI {

    public static final Log log = LogFactory.getLog(TestFileLevelAPI.class);

    public void createFile(String fName) throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path(fName);
        FSDataOutputStream out = fs.create(path);
        //
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(out));
        for (int i=0; i<15; i++) {
            bw.write("New Record " + i);
            bw.newLine();
        }
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

    public void getMetadataForDirectory(String dName) throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path(dName);
        String pathOnHadoop = dName;
        if (dName.equalsIgnoreCase("/"))
            pathOnHadoop = fs.getWorkingDirectory() + "/";
        //
        FileStatus[] fStatus = fs.listStatus(new Path(pathOnHadoop));
        for (FileStatus st : fStatus) {
            log.info("File Path Name : " + st.getPath().toString());
            log.info("Owner of the file : " + st.getOwner());
            log.info("File Len : " + st.getLen());
            log.info("File Block Size : " + st.getBlockSize());
            log.info("File Group: " + st.getGroup());
            log.info("File Permission : " + st.getPermission());
            log.info("File Access Time : " + st.getAccessTime());
            log.info("File Modification Time : " + st.getModificationTime());
            log.info("isDirectory flag : " + st.isDirectory());

        }
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
        log.info("File Len : " + fStatus.getLen());
        log.info("File Block Size : " + fStatus.getBlockSize());
    }

    public static void main(String[] args) throws Exception {
        //new TestFileLevelAPI().getBlockInfo(args[0]);
        //new TestFileLevelAPI().getMetadataForDirectory("Repos/HDFS_Hadoop/FileLevelAPI/file-02");
        new TestFileLevelAPI().getMetadataForDirectory(args[0]);
    }

}
