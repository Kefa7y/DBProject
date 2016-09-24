package mohamedAshraf_MohamedShokr;

import java.util.Hashtable;

public class test {
public static void main(String[] args) throws DBEngineException, DBAppException {

	DBApp myDB = new DBApp();
	myDB.init();
	Hashtable<String, String> fTblColNameType = new Hashtable<String, String>();
	fTblColNameType.put("ID", "Integer");
	fTblColNameType.put("Name", "String");

	Hashtable<String, String> fTblColNameRefs = new Hashtable<String, String>();

	myDB.createTable("Faculty", fTblColNameType, fTblColNameRefs, "ID");
	
	Hashtable<String, Object> ftblColNameValue1 = new Hashtable<String, Object>();
	ftblColNameValue1.put("ID", Integer.valueOf("1"));
	ftblColNameValue1.put("Name", "Media Engineering and Technology");
	myDB.insertIntoTable("Faculty", ftblColNameValue1);

	Hashtable<String, Object> ftblColNameValue2 = new Hashtable<String, Object>();
	ftblColNameValue2.put("ID", Integer.valueOf("2"));
	ftblColNameValue2.put("Name", "Management Technology");
	myDB.insertIntoTable("Faculty", ftblColNameValue2);
	
	myDB.viewTable("Faculty");
	
	Hashtable<String, Object> h = new Hashtable<String, Object>();
	h.put("ID", Integer.valueOf("2"));
	h.put("Name", "IET");
	myDB.updateTable("Faculty", "ID", h);	
	
	myDB.viewTable("Faculty");
	
	Hashtable<String, Object> h1 = new Hashtable<String, Object>();
	h1.put("ID", Integer.valueOf("2"));
	h1.put("Name", "IET");
	myDB.deleteFromTable("Faculty", h1, "AND");
	
	myDB.viewTable("Faculty");
}
}
