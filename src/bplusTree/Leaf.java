package bplusTree;

import java.util.ArrayList;
import java.util.Collections;

public class Leaf implements Node {

	Leaf seq;
	ArrayList<LeafValue> values;

	public Leaf(Leaf seq) {
		this.seq = seq;
		this.values = new ArrayList<LeafValue>();
	}

	public boolean overflow() {
		return values.size() == Bplus.n+1;
	}

	@SuppressWarnings("unchecked")
	public NonLeafValue add(Comparable value , Integer pNum , Integer index) {
		values.add(new LeafValue(value,pNum,index));
		Collections.sort(values);
		if(overflow()){
			Leaf x = new Leaf(seq);
			seq = x;
			for(int i = Bplus.n;i>=(Bplus.n/2);i--){
				x.values.add(this.values.get(i));
				this.values.remove(i);
			}
			Collections.sort(x.values);
			return new NonLeafValue(x,x.values.get(0).value);
		}
		else return null;
	}
	
	public String toString(){
		return ""+this.values;
	}

	public LeafValue find(Comparable x){
		for(int i=0;i<values.size();i++){
			if(values.get(i).value.equals(x) && values.get(i).exists)
				return values.get(i);
		}
		return null;
	}
}
