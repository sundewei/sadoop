package com.sap.hadoop.index;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: Jan 18, 2011
 * Time: 4:03:13 PM
 * To change this template use File | Settings | File Templates.
 */
public class LuceneIndexRecordWriter<K, V> extends RecordWriter<K, V> {

    private IndexWriter indexWriter;
    private String indexHdfsDirectory;
    private Directory ramDir;
    private FileSystem fileSystem;

    public LuceneIndexRecordWriter() throws IOException, InterruptedException {
        Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_30);
        ramDir = new RAMDirectory();
        // Creating IndexWriter object for the Ram Directory
        indexWriter = new IndexWriter(ramDir, analyzer, IndexWriter.MaxFieldLength.UNLIMITED);
    }

    public void setIndexHdfsDirectory(String indexHdfsDirectory) {
        this.indexHdfsDirectory = indexHdfsDirectory;
    }

    public void setFileSystem(FileSystem fileSystem) {
        this.fileSystem = fileSystem;
    }

    public void write(K key, V value) throws IOException, InterruptedException {
        boolean nullKey = key == null || key instanceof NullWritable;
        boolean nullValue = value == null || value instanceof NullWritable;
        Document document = new Document();
        if (!nullKey && !nullValue) {
            document.add(new Field("keyword", key.toString(), Field.Store.YES, Field.Index.ANALYZED));
            String commaSaperatedUrls = value.toString();
            StringTokenizer st = new StringTokenizer(commaSaperatedUrls, ",");
            int urlIdx = 0;
            while (st.hasMoreTokens()) {
                document.add(new Field("url_" + urlIdx, st.nextToken(), Field.Store.YES, Field.Index.ANALYZED));
                urlIdx++;
            }
            indexWriter.addDocument(document);
        }
    }

    /**
     * Close this <code>RecordWriter</code> to future operations.
     *
     * @param context the context of the task
     * @throws IOException
     */
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        indexWriter.optimize();
        indexWriter.commit();
        writeToDisk(fileSystem, ramDir, indexHdfsDirectory);
        indexWriter.close();
    }

    private static void writeToDisk(FileSystem dfs, Directory dir, String pathString) throws IOException {
        // Getting files present in memory into an array.
        String[] fileList = dir.listAll();

        // Reading index files from memory and storing them to HDFS.
        for (String idxFilename : fileList) {
            IndexInput indxfile = dir.openInput(idxFilename.trim());
            long len = indxfile.length();
            int len1 = (int) len;

            // Reading data from file into a byte array.
            byte[] bytarr = new byte[len1];
            indxfile.readBytes(bytarr, 0, len1);

            // Creating file in HDFS directory with name same as that of
            //index file
            if (!pathString.endsWith(Path.SEPARATOR)) {
                pathString = pathString + Path.SEPARATOR;
            }
            String writeToPathString = pathString + idxFilename.trim();
            Path src = new Path(writeToPathString);
            dfs.createNewFile(src);

            // Writing data from byte array to the file in HDFS
            FSDataOutputStream fs = dfs.create(src, true);
            fs.write(bytarr);
            fs.close();
        }
    }
}
