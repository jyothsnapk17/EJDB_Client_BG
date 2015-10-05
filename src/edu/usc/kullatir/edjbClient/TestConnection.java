package edu.usc.kullatir.edjbClient ; 

import java.util.Date;
import java.awt.image.*;

import org.ejdb.bson.BSONObject;
import org.ejdb.driver.EJDB;
import org.ejdb.driver.EJDBCollection;
import org.ejdb.driver.EJDBCollection.Index;
import org.ejdb.driver.EJDBQueryBuilder;
import org.ejdb.driver.EJDBResultSet;
import org.ejdb.*;

public class TestConnection {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		EJDB ejdbConn = new EJDB() ; 
		try {
			ejdbConn.open("BG_DB", EJDB.JBOREADER | EJDB.JBOWRITER | EJDB.JBOCREAT | EJDB.JBOTRUNC);
			
			System.out.println(ejdbConn.getPath());
			
			Date dob = new Date(1991,02,17) ; 
			Date joinDate = new Date(2015, 04, 15) ; 
			Date ld = new Date(2015, 10, 05, 1, 53, 34) ; 
            
			BSONObject user1 = new BSONObject("userid", "1")
					.append("username", "Jyothsna Kullatira")
                    .append("pw", "abcd")
                    .append("fname", "Jyothsna")
                    .append("lname", "Kullatira")
                    .append("gender", "F")
                    .append("dob", dob)
                    .append("jdate", joinDate )
                    .append("ldate", ld)
                    .append("address", "1226 2222222")
                    .append("email", "kullatir@usc.edu") 
                    .append("tel", "(213)345-3555") 
                    .append("pic", null)
                    .append("tpic", null) ;
             
			dob.UTC(1989, 11, 11, 0, 0, 0);
			joinDate.UTC(2003, 1, 23, 12, 56, 23);
			ld.UTC(2015, 10, 05, 1, 53, 34);
			BSONObject user2 = new BSONObject("userid", "2")
					.append("username", "Ayesha Amrin")
                    .append("pw", "wxyz")
                    .append("fname", "Ayesha")
                    .append("lname", "Amrin")
                    .append("gender", "F")
                    .append("dob", dob)
                    .append("jdate", joinDate)
                    .append("ldate", ld)
                    .append("address", "1226 2222222")
                    .append("email", "ayesha.amrin@gmail.com") 
                    .append("tel", "(213)416-7455")
                    .append("pic", null)
                    .append("tpic", null) ;
			
			BSONObject resource1 = new BSONObject("rid", "1")
					.append("creatorid", "1")
					.append("walluserid", "2")
					.append("type", "comment")
					.append("body", null)
					.append("doc", new Date()) ; 
			
			Date currDate = new Date() ; 
			BSONObject manipulation1 = new BSONObject("mid", "1")
					.append("creatorid", "1")
					.append("rid", "1")
					.append("modifierid", "1")
					.append("timestamp", currDate.getTime())
					.append("type", "rating")
					.append("content", "dddddd")  ;
			
			BSONObject friendship1 = new BSONObject("requesterid", "1")
					.append("receiverid", "2")
					.append("status", "asked") ; 
			

            
            System.out.println("user1 =\n\t\t" + user1);
            System.out.println("user2 =\n\t\t" + user2);

            EJDBCollection users = ejdbConn.getCollection("Users") ; 
            
            users.save(user1) ; 
            users.ensureIStringIndex("userid");
            users.ensureStringIndex("fname");
            users.save(user2) ;
            users.rebuildStringIndex("userid");
            users.rebuildStringIndex("fname");

            //            users.rebuildStringIndex("lname");
            
            
            java.util.Collection<Index> userIdx = users.getIndexes();
            for(Index i:userIdx) { 
            	System.out.println("Name -> " + i.getName() + "\t Field -> " + i.getField());
            }
            
            EJDBCollection resources = ejdbConn.ensureCollection("Resources") ; 
            
            resources.save(resource1) ; 
            
            EJDBCollection manipulations = ejdbConn.ensureCollection("Manipulations") ; 
            manipulations.save(manipulation1) ; 
            
            EJDBCollection friendships = ejdbConn.ensureCollection("Friendships") ; 
            friendships.save(friendship1) ; 
            
            EJDBQueryBuilder qb ; 
            EJDBResultSet rs;
            //Queries
            
            //Query1
            qb = new EJDBQueryBuilder() ; 
            qb.field("userid", "1") ;  
            
            rs = users.createQuery(qb).find();
            System.out.println();
            System.out.println("Results (Q1): " + rs.length());
            for (BSONObject r : rs) {
                System.out.println("\t" + r.get("fname"));
            }
           
            rs.close();
            
		} finally {
			if(ejdbConn.isOpen()){
				
				ejdbConn.close();
			}
		}
	}

}
