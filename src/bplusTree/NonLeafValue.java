package bplusTree;

@SuppressWarnings("rawtypes")
public class NonLeafValue implements Comparable{
	
	Node pointer;
	Comparable value;
	
	public NonLeafValue(Node pointer , Comparable value){
		this.pointer=pointer;
		this.value=value;
	}

	@Override
	public int compareTo(Object o) {
		NonLeafValue n = (NonLeafValue) o;
		return this.value.compareTo(n.value);
	}

	public String toString(){
		return ""+value;
	}
}
