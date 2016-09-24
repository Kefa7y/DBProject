package mohamedAshraf_MohamedShokr;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;

@SuppressWarnings("serial")
public class Page implements Serializable {

	Row[] data;
	boolean[] exists;
	int index;
	int pNum;
	String tbName;
	// maximum number of rows
	static int n = 200;

	public Page(String tbName, int pNum) throws DBAppException {
		data = new Row[Page.n];
		exists = new boolean[Page.n];
		index = 0;
		this.pNum = pNum;
		this.tbName = tbName;
		try {
			ObjectOutputStream oos = new ObjectOutputStream(
					new FileOutputStream(new File("classes/" + tbName + pNum + ".class")));
			oos.writeObject(this);
			oos.close();
		} catch (IOException e) {
			throw new DBAppException("Error writing the page file.");
		}
	}

	public void add(Hashtable<String, Object> colNameValue) throws DBAppException {

		try {
			BufferedReader br = new BufferedReader(new FileReader("data/meta-data.csv"));
			String line;
			while ((line = br.readLine()) != null) {
				String[] s = line.split(",");
				if (s[0] == this.tbName) {
					Object value = colNameValue.get(s[1]);
					if (value.getClass() != Class.forName(s[2]))
						throw new DBAppException("Incompatible types.");
				}
			}
			br.close();
		} catch (IOException | ClassNotFoundException e) {
			e.printStackTrace();
			throw new DBAppException("meta-data.csv was not found");
		}
		data[index] = new Row(colNameValue);
		exists[index++] = true;
		try {
			ObjectOutputStream oos = new ObjectOutputStream(
					new FileOutputStream(new File("classes/" + tbName + pNum + ".class")));
			oos.writeObject(this);
			oos.close();
		} catch (IOException e) {
			throw new DBAppException("Error writing the page file.");
		}
	}

	public void select(ArrayList<Row> result, Hashtable<String, Object> colNameValue, String operator) {
		
		for (int i = 0; i < index; i++) {
			Iterator<Map.Entry<String, Object>> it = colNameValue.entrySet().iterator();
			boolean f;
			if (operator.equals("AND"))
				f = true;
			else
				f = false;
			while (it.hasNext()) {
				Map.Entry<String, Object> entry = it.next();
				if (operator.equals("AND"))
					f &= data[i].data.get(entry.getKey()).equals(entry.getValue());
				if (operator.equals("OR"))
					f |= data[i].data.get(entry.getKey()).equals(entry.getValue());
			}
			if (f && exists[i])
				result.add(data[i]);
		}
	}

	public void delete(Hashtable<String, Object> colNameValue, String operator) throws DBAppException {
		for (int i = 0; i < index; i++) {
			Iterator<Map.Entry<String, Object>> it = colNameValue.entrySet().iterator();
			boolean f;
			if (operator.equals("AND"))
				f = true;
			else
				f = false;
			while (it.hasNext()) {
				Map.Entry<String, Object> entry = it.next();
				if (operator.equals("AND"))
					f &= data[i].data.get(entry.getKey()).equals(entry.getValue());
				if (operator.equals("OR"))
					f |= data[i].data.get(entry.getKey()).equals(entry.getValue());
			}
			if (f) {
				exists[i] = false;
			}

		}
		try {
			ObjectOutputStream oos = new ObjectOutputStream(
					new FileOutputStream(new File("classes/" + tbName + pNum + ".class")));
			oos.writeObject(this);
			oos.close();
		} catch (IOException e) {
			throw new DBAppException("Error writing the page file.");
		}
	}

	public void update(Hashtable<String, Object> colNameValue, String strKey) throws DBAppException {
		for (int i = 0; i < index; i++) {
			if (data[i].data.get(strKey).equals(colNameValue.get(strKey))&&exists[i]) {
				Iterator<Map.Entry<String, Object>> it = colNameValue.entrySet().iterator();
				while (it.hasNext()) {
					Map.Entry<String, Object> entry = it.next();
					data[i].data.put(entry.getKey(), entry.getValue());
				}
				try {
					ObjectOutputStream oos = new ObjectOutputStream(
							new FileOutputStream(new File("classes/" + tbName + pNum + ".class")));
					oos.writeObject(this);
					oos.close();
				} catch (IOException e) {
					throw new DBAppException("Error writing the page file.");
				}
			}
		}
	}
	
	public Row get(int i){
		return data[i];
	}

	public String toString() {
		String res = "";
		for (int i = 0; i <index; i++)
			if(exists[i])
				res += data[i];
		return res;
	}

	public boolean isFull() {
		return index == n;
	}

}
