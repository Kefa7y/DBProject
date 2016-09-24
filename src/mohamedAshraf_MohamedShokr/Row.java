package mohamedAshraf_MohamedShokr;
import java.io.Serializable;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Hashtable;

@SuppressWarnings("serial")
public class Row implements Serializable {

	public Hashtable<String, Object> data;

	public Row(Hashtable<String, Object> data) {
		DateFormat dateFormat = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
		Date date = new Date();
		data.put("TouchDate", dateFormat.format(date));
		this.data = data;
	}

	public String toString() {
		return "" + data.toString();
	}

	public static void main(String[] args) {
		Hashtable<String, Object> h = new Hashtable<>();
		h.put("ID", new Integer(1));
		h.put("name", "Slim");
		System.out.println(h);
		Row r = new Row(h);
		System.out.println(h);
		System.out.println(r);
	}
}
