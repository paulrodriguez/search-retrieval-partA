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
	    writer = new IndexWriter(System.getenv("LUCENE_INDEX")+"/ebay-index", 
				new StandardAnalyzer(), create);
		}
		return writer;
    }
    
    public void closeIndexWriter() throws Exception {
		if(writer != null) {
			writer.close();
		}
    }
	
    public void indexItems(ResultSet items, ArrayList item_cats) throws Exception {
		IndexWriter w = getIndexWriter(false);
		Document doc = new Document();
		String id = items.getString("ItemID");
		String name = items.getString("Name");
		String des = items.getString("Description");
		
		//this will allow to split the categories based on the delimiter given
		//String[] category_parts = categories.split("|||");
		
		/*
		int cat_cnt = 0; // counter for category results
		while(categories.next())
		{
			category_parts[cat_cnt] = categores.getString("Category");
			System.out.println(category_parts[cat_cnt]);
			*/
			/*
			String categories = "";
			for(String cat : cats) // loop through category results
			{
				categories += cat + "|||"; // using ||| as delimiter here, may need to change
			}
		}
			*/

		//will need to change for the values from the result of the queries 
		doc.add(new Field("ItemID", id, Field.Store.YES, Field.Index.NO));
		doc.add(new Field("Name", name, Field.Store.YES, Field.Index.TOKENIZED));
		/*probably will not need to store them since we only return itemid and name*/
		doc.add(new Field("Description", des, Field.Store.NO, Field.Index.TOKENIZED));

		for(int i=0; i<item_cats.size(); i++)
		{
			// this adds a new Field "Category" for every single category.
			// Not sure if this works in Lucene--you will probably have to change 
			// this from items_cats.get(i), to some string that concatenates all
			// of the categories
			doc.add(new Field("Category", item_cats.get(i).toString(), Field.Store.NO, Field.Index.UN_TOKENIZED));
			//doc.add(new Field("Category", category_parts[i], Field.Store.NO, Field.Index.UN_TOKENIZED));
		}
		//doc.add(new Field("Category", categories, Field.Store.NO, Field.Index.TOKENIZED));
		/*
		for (int i = 0; i < category_parts.length; i++) {
			doc.add(new Field("Category", category_parts[i], Field.Store.NO, Field.Index.UN_TOKENIZED));
		}
		*/
		//for this we need to concatenate Name, Description, and Categories 
		//String fullSearch = name+ " " + des + categories;
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
			//will make an index on the itemid, cat
			Statement stmt1 = conn.createStatement();

			//here we need to get the itemid name and description
			//change query depending on who's implementation is used
			ResultSet items  = stmt1.executeQuery("SELECT ItemID, Name, Description FROM Items ORDER BY ItemID");
		
			Statement stmt2 = conn.createStatement();
			//change query if needed
			/*
			ResultSet categories  = stmt2.executeQuery("SELECT * FROM Categories ORDER BY ItemID");
		
			HashMap<Integer, String> cats = new HashMap<Integer, String>();
			//create a hasmap to store categories in a string for each item
			while(categories.next()) {
				int iid = categories.getInt("ItemID");
				String category = categories.getString("Category");
				if(cats.containsKey(iid)) {
					cats.put(iid, cats.get(iid)+"|||"+category);
				}
				else {
					cats.put(iid,category);
				}
			}
		
		
		   
			//need to somehow get all the categories for each item and store them
			//*/
		
			while(items.next()) {
				ArrayList item_cats = new ArrayList(); // hold correspondies categories for an item

				ResultSet item_cats_rs = stmt2.executeQuery(
						"SELECT * FROM Categories WHERE ItemID = " + 
						items.getInt("ItemID") + ";");
				while(item_cats_rs.next())
				{
					item_cats.add(item_cats_rs.getString("Category"));
				}

				//indexItems(items, cats.get(items.getInt("ItemID")));
				indexItems(items, item_cats);
			}

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
