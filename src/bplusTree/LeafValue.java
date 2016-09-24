package bplusTree;

@SuppressWarnings("rawtypes")
public class LeafValue implements Comparable {

	Comparable value;
	Integer pNum;
	Integer index;
	boolean exists;
	
	public LeafValue(Comparable value,Integer pNum,Integer index){
		this.value=value;
		this.pNum=pNum;
		this.index=index;
		exists=true;
	}

	@Override
	public int compareTo(Object o) {
		LeafValue n = (LeafValue) o;
		return this.value.compareTo(n.value);
	}

	public String toString(){
		return ""+value;
	}
}
