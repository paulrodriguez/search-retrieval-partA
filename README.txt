PAUL RODRIGUEZ -303675125
JACQUELINE LO - 203943529
project 3a

Using Paul's project 2 files/set-up.

search queries will be done with these set of attributes:
item name, category, seller, buy price, bidder, ending time and description.

we decided to create SQL indices on seller, bidder, buy price and ending time because they can be compared easily with equality conditions.

the seller, buy price, and ending time indices are created on the attributes of our Items Table. Although our Users table contains the UserID as the primary key, which applies to both seller ids and bidder ids, we felt it was best to apply an index on sellers and bidders on Items and Bids tables, respectively, because it would be more efficient since it will probably be easier to access data this way. 


the attributes item name, category, and description will be indexed using the inverted indices provided by Lucene since they require word comparison and checking if a word might be contained in one of these attributes.

the item name,category and description will be tokenized, and there is a 'content' index that joins all three of them and tokenizes them for searching