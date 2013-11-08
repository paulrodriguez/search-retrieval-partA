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
/*
// for testing:
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermEnum;
//end testing
*/
import java.util.HashMap;
import java.util.ArrayList;

public class Indexer {
    
    /** Creates a new instance of Indexer */
    public Indexer() {
    }

    private IndexWriter writer = null;

    //erases the index and creates a new one
    public IndexWriter getIndexWriter(boolean create) throws Exception {
	if (writer == null) {
	    writer = new IndexWriter(System.getenv("LUCENE_INDEX") + "/ebay-index", 
				new StandardAnalyzer(), create);
		}
		return writer;
    }
    
    public void closeIndexWriter() throws Exception {
		if(writer != null) {
			writer.close();
		}
    }
	
    public void indexItems(ResultSet items, String item_cats) throws Exception {
		
		IndexWriter w = getIndexWriter(false);
		
		Document doc = new Document();
		
		String id = items.getString("ItemID");
		String name = items.getString("Name");
		String des = items.getString("Description");

		doc.add(new Field("ItemID", id, Field.Store.YES, Field.Index.NO));
		
		doc.add(new Field("Name", name, Field.Store.YES, Field.Index.TOKENIZED));
		// probably will not need to store them since we only return itemid and name
		doc.add(new Field("Description", des, Field.Store.NO, Field.Index.TOKENIZED));

		/**String catString = ""; // concatenate all categories for content string
		for(int i=0; i<item_cats.size(); i++)
		{
			// this adds a new duplicate Field "Category" for every single category.
			//doc.add(new Field("Category", item_cats.get(i).toString(), 
				//		Field.Store.NO, Field.Index.UN_TOKENIZED));

			catString += " " + item_cats.get(i); // to add onto fullSearchableText
		}*/
		
		doc.add(new Field("Category", item_cats, Field.Store.NO, Field.Index.TOKENIZED));
		//doc.add(new Field("Category", catString, Field.Store.NO, Field.Index.TOKENIZED));
	//	System.out.println(catString);
		// Composite field 
		//String fullSearchableText = name + " "+ des + " " + catString;
		String fullSearchableText = name + " "+ des + " " + item_cats;
		doc.add(new Field("content", fullSearchableText, Field.Store.NO, 
					Field.Index.TOKENIZED));

		writer.addDocument(doc);
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
		try {
			getIndexWriter(true);
			//used to create a query to Items table
			Statement stmt1 = conn.createStatement();

			// this gets the tuples with itemid name and description
			ResultSet items  = stmt1.executeQuery("SELECT ItemID, Name, Description FROM Items ORDER BY ItemID");
		
			// used to execute queries to Categories table
			Statement stmt2 = conn.createStatement();
			//String item_cats = "";
			while(items.next()) {
				//ArrayList<String> item_cats = new ArrayList<String>(); // hold corresponding categories for an item
				String item_cats = "";
				ResultSet item_cats_rs = stmt2.executeQuery(
						"SELECT * FROM Categories WHERE ItemID = " + 
						items.getInt("ItemID") + ";");
				while(item_cats_rs.next())
				{
					item_cats += item_cats_rs.getString("Category") + " ";
				//item_cats.add(item_cats_rs.getString("Category"));
				}
				item_cats_rs.close();

				// give ResultSet's to helper function for indexing
				indexItems(items, item_cats);
			}

			/*
			// for testing
			System.out.println("-----------------------------------------------------------------------------------------\n\n\n\n\n");
			System.out.println("Here are the indices that have been created: ");
			IndexReader r=IndexReader.open(System.getenv("LUCENE_INDEX") + "/ebay-index");
			TermEnum terms=r.terms();
			System.out.println("num docs: "+r.numDocs());
			while(terms.next())
			{
				Term t=terms.term();
			System.out.println(t.text());
			}
			terms.close(); r.close();
			//end testing
			*/
			items.close();
			stmt1.close();
			stmt2.close();
	
			closeIndexWriter();
		} catch (Exception e) {
			e.printStackTrace();
		}

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
