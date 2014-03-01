package com.esri.hadoop;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

import static org.apache.log4j.Level.ERROR;

/**
 */
public class MiniFS
{
    protected FileSystem m_fileSystem;
    protected JobConf m_jobConfig;
    protected MiniDFSCluster m_dfsCluster;
    protected FSDataOutputStream m_dataOutputStream;
    protected FSDataInputStream m_dataInputStream;
    protected Path m_path;

    public void setupMetricsLogging()
    {
        Logger.getLogger(org.apache.hadoop.metrics2.util.MBeans.class).setLevel(ERROR);
        Logger.getLogger(org.apache.hadoop.metrics2.impl.MetricsSystemImpl.class).setLevel(ERROR);
    }

    @Before
    public void setUp() throws Exception
    {
        final File tmpDir = File.createTempFile("dfs", "");

        // used by MiniDFSCluster for DFS storage
        System.setProperty("test.build.data", new File(tmpDir, "data").getAbsolutePath());

        // required by JobHistory.initLogDir
        System.setProperty("hadoop.log.dir", new File(tmpDir, "logs").getAbsolutePath());

        setupMetricsLogging();

        final Configuration config = new Configuration();
        config.set("hadoop.tmp.dir", tmpDir.getAbsolutePath());
        config.setBoolean("dfs.permissions", false);
        config.setInt("dfs.replication", 1);
        config.set("dfs.datanode.data.dir.perm", "777");

        if (tmpDir.exists())
        {
            FileUtils.forceDelete(tmpDir);
        }
        FileUtils.forceMkdir(tmpDir);

        m_jobConfig = new JobConf(config);
        m_dfsCluster = new MiniDFSCluster.Builder(m_jobConfig).numDataNodes(1).format(true).build();
        m_fileSystem = m_dfsCluster.getFileSystem();
        m_dfsCluster.waitClusterUp();
        m_path = new Path("/tmp", UUID.randomUUID().toString());
    }

    protected void openInputStream() throws IOException
    {
        m_dataInputStream = m_fileSystem.open(m_path);
    }

    protected void openOutputStream() throws IOException
    {
        m_dataOutputStream = m_fileSystem.create(m_path, true);
    }

    @After
    public void tearDown() throws Exception
    {
        m_dfsCluster.shutdown();
    }

}
