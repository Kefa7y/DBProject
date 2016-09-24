package mohamedAshraf_MohamedShokr;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import bplusTree.Bplus;

@SuppressWarnings("serial")
class DBAppException extends Exception {
	public DBAppException(String message) {
		super(message);
	}
}

@SuppressWarnings("serial")
class DBEngineException extends Exception {
	public DBEngineException(String message) {
		super(message);
	}
}

public class DBApp {

	public void init() throws DBEngineException {

		// initialize meta-data if it is not there already
		File f = new File("data/meta-data.csv");
		if (!f.exists() || !f.isDirectory()) {
			try {
				FileWriter filewriter = new FileWriter("data/meta-data.csv");
				filewriter.append("Table Name, Column Name, Column Type, Key, Indexed, References\n");
				filewriter.flush();
				filewriter.close();
			} catch (IOException e) {
				throw new DBEngineException("Problems finding meta-data file.");
			}
		}
		File config = new File("config/DBApp.config");
		try {
			FileReader rdr = new FileReader(config);
			Properties p = new Properties();
			p.load(rdr);
			Page.n = Integer.parseInt(p.getProperty("MaximumRowsCountinPage"));
			Bplus.n = Integer.parseInt(p.getProperty("BPlusTreeN"));
			rdr.close();
		} catch (IOException e) {
			throw new DBEngineException("Problems finding meta-data file.");
		}

	}

	public void createTable(String strTableName, Hashtable<String, String> htblColNameType,
			Hashtable<String, String> htblColNameRefs, String strKeyColName) throws DBAppException, DBEngineException {
		try {
			BufferedReader br = new BufferedReader(new FileReader("data/meta-data.csv"));
			String line = "";
			while ((line = br.readLine()) != null) {
				String[] s = line.split(",");
				if (s[0].equals(strTableName))
					throw new DBAppException("Table already exists.");
			}
			br.close();
			if (htblColNameType.isEmpty() || htblColNameType == null)
				throw new DBEngineException("No columns are specified for the table.");
			else {
				FileWriter filewriter = new FileWriter(new File("data/meta-data.csv"), true);
				Iterator<Map.Entry<String, String>> it = htblColNameType.entrySet().iterator();
				while (it.hasNext()) {
					Map.Entry<String, String> entry = it.next();
					boolean f = false;
					if (entry.getKey().equals(strKeyColName))
						f = true;
					String s = entry.getValue();
					switch (s) {
					case "Integer":
					case "Double":
					case "String":
					case "Boolean":
						s = "java.lang." + s;
						break;
					case "Date":
						s = "java.util." + s;
						break;
					default:
						throw new DBEngineException("Type not supported");
					}
					filewriter.append(strTableName + "," + entry.getKey() + "," + s + "," + f + "," + "false" + ",");
					boolean r = false;
					if (htblColNameRefs != null && !htblColNameRefs.isEmpty()) {
						Iterator<Map.Entry<String, String>> it2 = htblColNameRefs.entrySet().iterator();
						while (it2.hasNext()) {
							Map.Entry<String, String> ref = it2.next();
							if (ref.getKey().equals(entry.getKey())) {
								String refTbName = "";
								String refColName = "";
								for (int i = 0; i < ref.getValue().length(); i++)
									if (ref.getValue().charAt(i) == '.') {
										refTbName = ref.getValue().substring(0, i);
										refColName = ref.getValue().substring(i + 1);
									}
								br = new BufferedReader(new FileReader("data/meta-data.csv"));
								boolean x = false;
								while ((line = br.readLine()) != null) {
									String[] s2 = line.split(",");
									if (s2[0].equals(refTbName) && s2[1].equals(refColName)) {
										x = true;
										break;
									}
								}
								if (!x)
									throw new DBAppException("The column you referenced does not exist.");
								br.close();
								filewriter.append(ref.getValue() + "\n");
								it.remove();
								r = true;
								break;
							}
						}
					}
					if (!r)
						filewriter.append("false\n");
				}
				new Page(strTableName, 0);
				filewriter.flush();
				filewriter.close();
			}
		} catch (IOException e) {
			throw new DBEngineException("An IO error has occured");
		}
	}

	public void createIndex(String strTableName, String strColName) throws DBAppException {

	}

	public void insertIntoTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException {
		Page p = null;
		int i = 0;
		while (true) {
			ObjectInputStream ois;
			try {
				ois = new ObjectInputStream(new FileInputStream(new File("classes/" + strTableName + i + ".class")));
				p = (Page) ois.readObject();
				ois.close();
			} catch (Exception e) {
				throw new DBAppException("Error reading page files.");
			}
			i++;
			if (p.isFull())
				if (new File("classes/" + strTableName + i + ".class").exists())
					continue;
				else
					p = new Page(strTableName, i);
			else
				break;
		}
		p.add(htblColNameValue);
	}

	public void updateTable(String strTableName, String strKey, Hashtable<String, Object> htblColNameValue)
			throws DBAppException {

		int i = 0;
		while (true) {
			File f = new File("classes/" + strTableName + i + ".class");
			if (f.exists()) {
				try {
					ObjectInputStream ois = new ObjectInputStream(
							new FileInputStream(new File("classes/" + strTableName + i + ".class")));
					Page p = (Page) ois.readObject();
					ois.close();
					p.update(htblColNameValue, strKey);
					i++;
				} catch (Exception e) {
					e.printStackTrace();
					throw new DBAppException("Error reading page files.");
				}
			} else {
				return;
			}
		}
	}

	public void deleteFromTable(String strTableName, Hashtable<String, Object> htblColNameValue, String strOperator)
			throws DBEngineException, DBAppException {
		
		int i = 0;
		while (true) {
			File f = new File("classes/" + strTableName + i + ".class");
			if (f.exists()) {
				try {
					ObjectInputStream ois = new ObjectInputStream(
							new FileInputStream(new File("classes/" + strTableName + i + ".class")));
					Page p = (Page) ois.readObject();
					ois.close();
					p.delete(htblColNameValue, strOperator);
					i++;
				} catch (Exception e) {
					e.printStackTrace();
					throw new DBAppException("Error reading page files.");
				}
			} else {
				return;
			}
		}

	}

	public void viewTable(String strTableName)throws DBEngineException, DBAppException {		
		int i = 0;
		while (true) {
			File f = new File("classes/" + strTableName + i + ".class");
			if (f.exists()) {
				try {
					ObjectInputStream ois = new ObjectInputStream(
							new FileInputStream(new File("classes/" + strTableName + i + ".class")));
					Page p = (Page) ois.readObject();
					ois.close();
					System.out.println(p);
					i++;
				} catch (Exception e) {
					e.printStackTrace();
					throw new DBAppException("Error reading page files.");
				}
			} else {
				return;
			}
		}

	}
	
	public Iterator<Row> selectFromTable(String strTable, Hashtable<String, Object> htblColNameValue,
			String strOperator) throws DBEngineException, DBAppException {
		int i = 0;
		ArrayList<Row> x = new ArrayList<Row>();
		while (true) {
			File f = new File("classes/" + strTable + i + ".class");
			if (f.exists()) {
				try {
					ObjectInputStream ois = new ObjectInputStream(
							new FileInputStream(new File("classes/" + strTable + i + ".class")));
					Page p = (Page) ois.readObject();
					ois.close();
					p.select(x, htblColNameValue, strOperator);
					i++;
				} catch (Exception e) {
					e.printStackTrace();
					throw new DBAppException("Error reading page files.");
				}
			} else {
				return x.iterator();
			}
		}
	}

	public static void main(String[] args) throws DBAppException, DBEngineException {
		// create a new DBApp
		DBApp myDB = new DBApp();

		// initialize it
		myDB.init();

		// creating table "Faculty"
	
		Hashtable<String, String> fTblColNameType = new Hashtable<String, String>();
		fTblColNameType.put("ID", "Integer");
		fTblColNameType.put("Name", "String");

		Hashtable<String, String> fTblColNameRefs = new Hashtable<String, String>();

		myDB.createTable("Faculty", fTblColNameType, fTblColNameRefs, "ID");

		// creating table "Major"

		Hashtable<String, String> mTblColNameType = new Hashtable<String, String>();
		mTblColNameType.put("ID", "Integer");
		mTblColNameType.put("Name", "String");
		mTblColNameType.put("Faculty_ID", "Integer");

		Hashtable<String, String> mTblColNameRefs = new Hashtable<String, String>();
		mTblColNameRefs.put("Faculty_ID", "Faculty.ID");

		myDB.createTable("Major", mTblColNameType, mTblColNameRefs, "ID");

		// creating table "Course"

		Hashtable<String, String> coTblColNameType = new Hashtable<String, String>();
		coTblColNameType.put("ID", "Integer");
		coTblColNameType.put("Name", "String");
		coTblColNameType.put("Code", "String");
		coTblColNameType.put("Hours", "Integer");
		coTblColNameType.put("Semester", "Integer");
		coTblColNameType.put("Major_ID", "Integer");

		Hashtable<String, String> coTblColNameRefs = new Hashtable<String, String>();
		coTblColNameRefs.put("Major_ID", "Major.ID");

		myDB.createTable("Course", coTblColNameType, coTblColNameRefs, "ID");

		// creating table "Student"

		Hashtable<String, String> stTblColNameType = new Hashtable<String, String>();
		stTblColNameType.put("ID", "Integer");
		stTblColNameType.put("First_Name", "String");
		stTblColNameType.put("Last_Name", "String");
		stTblColNameType.put("GPA", "Double");
		stTblColNameType.put("Age", "Integer");

		Hashtable<String, String> stTblColNameRefs = new Hashtable<String, String>();

		myDB.createTable("Student", stTblColNameType, stTblColNameRefs, "ID");

		// creating table "Student in Course"

		Hashtable<String, String> scTblColNameType = new Hashtable<String, String>();
		scTblColNameType.put("ID", "Integer");
		scTblColNameType.put("Student_ID", "Integer");
		scTblColNameType.put("Course_ID", "Integer");

		Hashtable<String, String> scTblColNameRefs = new Hashtable<String, String>();
		scTblColNameRefs.put("Student_ID", "Student.ID");
		scTblColNameRefs.put("Course_ID", "Course.ID");

		myDB.createTable("Student_in_Course", scTblColNameType, scTblColNameRefs, "ID");

		// insert in table "Faculty"

		Hashtable<String, Object> ftblColNameValue1 = new Hashtable<String, Object>();
		ftblColNameValue1.put("ID", Integer.valueOf("1"));
		ftblColNameValue1.put("Name", "Media Engineering and Technology");
		myDB.insertIntoTable("Faculty", ftblColNameValue1);

		Hashtable<String, Object> ftblColNameValue2 = new Hashtable<String, Object>();
		ftblColNameValue2.put("ID", Integer.valueOf("2"));
		ftblColNameValue2.put("Name", "Management Technology");
		myDB.insertIntoTable("Faculty", ftblColNameValue2);

		for (int i = 0; i < 1000; i++) {
			Hashtable<String, Object> ftblColNameValueI = new Hashtable<String, Object>();
			ftblColNameValueI.put("ID", Integer.valueOf(("" + (i + 2))));
			ftblColNameValueI.put("Name", "f" + (i + 2));
			myDB.insertIntoTable("Faculty", ftblColNameValueI);
		}

		// insert in table "Major"

		Hashtable<String, Object> mtblColNameValue1 = new Hashtable<String, Object>();
		mtblColNameValue1.put("ID", Integer.valueOf("1"));
		mtblColNameValue1.put("Name", "Computer Science & Engineering");
		mtblColNameValue1.put("Faculty_ID", Integer.valueOf("1"));
		myDB.insertIntoTable("Major", mtblColNameValue1);

		Hashtable<String, Object> mtblColNameValue2 = new Hashtable<String, Object>();
		mtblColNameValue2.put("ID", Integer.valueOf("2"));
		mtblColNameValue2.put("Name", "Business Informatics");
		mtblColNameValue2.put("Faculty_ID", Integer.valueOf("2"));
		myDB.insertIntoTable("Major", mtblColNameValue2);

		for (int i = 0; i < 1000; i++) {
			Hashtable<String, Object> mtblColNameValueI = new Hashtable<String, Object>();
			mtblColNameValueI.put("ID", Integer.valueOf(("" + (i + 2))));
			mtblColNameValueI.put("Name", "m" + (i + 2));
			mtblColNameValueI.put("Faculty_ID", Integer.valueOf(("" + (i + 2))));
			myDB.insertIntoTable("Major", mtblColNameValueI);
		}

		// insert in table "Course"

		Hashtable<String, Object> ctblColNameValue1 = new Hashtable<String, Object>();
		ctblColNameValue1.put("ID", Integer.valueOf("1"));
		ctblColNameValue1.put("Name", "Data Bases II");
		ctblColNameValue1.put("Code", "CSEN 604");
		ctblColNameValue1.put("Hours", Integer.valueOf("4"));
		ctblColNameValue1.put("Semester", Integer.valueOf("6"));
		ctblColNameValue1.put("Major_ID", Integer.valueOf("1"));
		myDB.insertIntoTable("Course", mtblColNameValue1);

		Hashtable<String, Object> ctblColNameValue2 = new Hashtable<String, Object>();
		ctblColNameValue2.put("ID", Integer.valueOf("1"));
		ctblColNameValue2.put("Name", "Data Bases II");
		ctblColNameValue2.put("Code", "CSEN 604");
		ctblColNameValue2.put("Hours", Integer.valueOf("4"));
		ctblColNameValue2.put("Semester", Integer.valueOf("6"));
		ctblColNameValue2.put("Major_ID", Integer.valueOf("2"));
		myDB.insertIntoTable("Course", mtblColNameValue2);

		for (int i = 0; i < 1000; i++) {
			Hashtable<String, Object> ctblColNameValueI = new Hashtable<String, Object>();
			ctblColNameValueI.put("ID", Integer.valueOf(("" + (i + 2))));
			ctblColNameValueI.put("Name", "c" + (i + 2));
			ctblColNameValueI.put("Code", "co " + (i + 2));
			ctblColNameValueI.put("Hours", Integer.valueOf("4"));
			ctblColNameValueI.put("Semester", Integer.valueOf("6"));
			ctblColNameValueI.put("Major_ID", Integer.valueOf(("" + (i + 2))));
			myDB.insertIntoTable("Course", ctblColNameValueI);
		}

		// insert in table "Student"

		for (int i = 0; i < 1000; i++) {
			Hashtable<String, Object> sttblColNameValueI = new Hashtable<String, Object>();
			sttblColNameValueI.put("ID", Integer.valueOf(("" + i)));
			sttblColNameValueI.put("First_Name", "FN" + i);
			sttblColNameValueI.put("Last_Name", "LN" + i);
			sttblColNameValueI.put("GPA", Double.valueOf("0.7"));
			sttblColNameValueI.put("Age", Integer.valueOf("20"));
			myDB.insertIntoTable("Student", sttblColNameValueI);
			// changed it to student instead of course
		}
		// selecting

		Hashtable<String, Object> stblColNameValue = new Hashtable<String, Object>();
		stblColNameValue.put("ID", Integer.valueOf("550"));
		stblColNameValue.put("Age", Integer.valueOf("20"));
		
		
		long startTime = System.currentTimeMillis();
		Iterator<Row> myIt = myDB.selectFromTable("Student", stblColNameValue, "AND");
		long endTime = System.currentTimeMillis();
		long totalTime = endTime - startTime;
		System.out.println(totalTime);
		while (myIt.hasNext()) {
			System.out.println(myIt.next());
		}

		// feel free to add more tests
		Hashtable<String, Object> stblColNameValue3 = new Hashtable<String, Object>();
		stblColNameValue3.put("Name", "m7");
		stblColNameValue3.put("Faculty_ID", Integer.valueOf("7"));

		long startTime2 = System.currentTimeMillis();
		Iterator<Row> myIt2 = myDB.selectFromTable("Major", stblColNameValue3, "AND");
		long endTime2 = System.currentTimeMillis();
		long totalTime2 = endTime2 - startTime2;
		System.out.println(totalTime2);
		while (myIt2.hasNext()) {
			System.out.println(myIt2.next());
		}
	}

}
