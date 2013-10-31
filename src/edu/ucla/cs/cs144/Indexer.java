package edu.ucla.cs.cs144;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.Document;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexWriter;

import java.util.AbstractMap;
public class Indexer {
    
    /** Creates a new instance of Indexer */
    public Indexer() {
    }
    private IndexWriter writer = null;
    //erases the index and creates a new one
    public IndexWriter getIndexWriter(boolean create) throws Exception {
	if (writer = null) {
	    writer = new IndexWriter(System.getenv("LUCENE_INDEX")+"/ebay-index",new StandardAnalyzer(), create );
	}
	return writer;
    }
    
    public void closeIndexWriter() throw Exception {
	if(writer != null) {
	    writer.close();
	}
    }

    public void indexItems(ResultSet items, String category) throws Exception {
	System.out.println("indexing Items");
	IndexWriter w = getIndexWriter(false);
	Document doc = new Document();
	String id = items.getString("ItemID");
	string name = items.getString("Name");
	string des = items.getString("Description");
	//will need to change for the values from the result of the queries 
	doc.add(new Field("ItemID", id, Field.Store.YES, Field.Index.NO));
	doc.add(new Field("Name", name, Field.Store.YES, Field.Index.TOKENIZED));
	/*probably will not need to store them since we only return itemid and name*/
	doc.add(new Field("Description", des, Field.Store.NO, Field.Index.TOKENIZED));
	doc.add(new Field("Category",category, Field.Store.NO, Field.Index.TOKENIZED));
	
	//for this we need to concatenate Name, Description, and Cateories 
	String fullSearch = name + " " + category + " " + des;
	doc.add(new Fields("searchText",fullSearch, Field.Store.NO, Field.Index.TOKENIZED));
    }

    public void rebuildIndexes() {

        Connection conn = null;
	
        // create a connection to the database to retrieve Items from MySQL
	try {
	    conn = DbManager.getConnection(true);
	} catch (SQLException ex) {
	    System.out.println(ex);
	}


	/*
	 * Add your code here to retrieve Items using the connection
	 * and add corresponding entries to your Lucene inverted indexes.
         *
         * You will have to use JDBC API to retrieve MySQL data from Java.
         * Read our tutorial on JDBC if you do not know how to use JDBC.
         *
         * You will also have to use Lucene IndexWriter and Document
         * classes to create an index and populate it with Items data.
         * Read our tutorial on Lucene as well if you don't know how.
         *
         * As part of this development, you may want to add 
         * new methods and create additional Java classes. 
         * If you create new classes, make sure that
         * the classes become part of "edu.ucla.cs.cs144" package
         * and place your class source files at src/edu/ucla/cs/cs144/.
	 * 
	 */

	getIndexWriter(true);
	//will make an index on the itemid, cat
	Statement st = conn.createStatement();

	//here we need to get the itemid name and description
	//change query depending on who's implementation is used
	ResultSet items  = st.executeQuery("SELECT ItemID, Name, Description FROM Items ORDER BY ItemID");
	
	//change query if needed
	ResultSet categories  = st.executeQuery("SELECT * FROM Categories ORDER BY ItemID");
	
	Map<Integer, String> cats = new HashMap<Integer, String>();
	
	while(categories.next()) {
	    int iid = categories.getInt("ItemID");
	    String category = categories.getString("Category");
	    if(cats.containsKey(iid)) {
		cats.put(iid, cats.get(iid)+" "+category);
	    }
	    else {
		cat.put(iid,cateogry);
	    }
	}
	
	
       
	//need to somehow get all the categories for each item and store them
	
	while(items.next()) {
	    
	    indexItems(items, cats.get(items.getInt("ItemID")));
	}

	closeIndexWriter();
        // close the database connection
	try {
	    conn.close();
	} catch (SQLException ex) {
	    System.out.println(ex);
	}
    }    

    public static void main(String args[]) {
        Indexer idx = new Indexer();
        idx.rebuildIndexes();
    }   
}
