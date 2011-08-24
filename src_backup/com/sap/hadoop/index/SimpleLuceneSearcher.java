package com.sap.hadoop.index;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.*;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

import java.io.File;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: I827779
 * Date: Jan 19, 2011
 * Time: 2:33:25 PM
 * To change this template use File | Settings | File Templates.
 */
public class SimpleLuceneSearcher {
    public static void main(String[] arg) throws Exception {
        FSDirectory fsDirectory = FSDirectory.open(new File("C:\\projects\\sadoop\\lucene_index"));
        Searcher searcher = new IndexSearcher(fsDirectory);
        Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_30);

        System.out.println("Total Documents = " + searcher.maxDoc());

        QueryParser parser = new QueryParser(Version.LUCENE_30, "keyword", analyzer);

        Query query = parser.parse("Table Tennis");

        TopDocs hits = searcher.search(query, 10);

        System.out.println("Number of matching documents = " + hits.totalHits);

        for (ScoreDoc scoreDoc : hits.scoreDocs) {
            Document doc = searcher.doc(scoreDoc.doc);
            List<Fieldable> fields = doc.getFields();
            for (Fieldable field : fields) {
                System.out.println(field.name() + ": " + field.stringValue());
            }
            System.out.println("\n\n\n");
        }
    }
}
