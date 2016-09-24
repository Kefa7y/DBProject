package bplusTree;

import java.util.ArrayList;
import java.util.Collections;

public class NonLeaf implements Node {

	ArrayList<NonLeafValue> values;
	Node lastPointer;
	
	public NonLeaf(){
		values = new ArrayList<NonLeafValue>();
	}
	
	public boolean overflow(){
		return values.size()==Bplus.n+1;
	}

	@SuppressWarnings("unchecked")
	public NonLeafValue add(Comparable value , Integer pNum, Integer index) {
		for(int i=0;i<values.size();i++){
			if((values.get(i).value).compareTo(value)>0){
				NonLeafValue n = values.get(i).pointer.add(value,pNum,index); 
				if(n==null)
					return null;
				else{
					Node temp= values.get(i).pointer;
					values.get(i).pointer = n.pointer;
					n.pointer = temp;
					values.add(n);
					Collections.sort(values);
					if(overflow()){
						//need to be done
						NonLeaf x = new NonLeaf();
						int y = Bplus.n/2;
						x.lastPointer=this.lastPointer;
						this.lastPointer=values.get(y).pointer;
						NonLeafValue returned = new NonLeafValue(x,this.values.get(y).value);
						values.remove(y);
						for(int j = y;j<this.values.size();){
							x.values.add(this.values.get(j));
							this.values.remove(j);
						}
						return returned;
					}
					else return null;
				}
			}
		}
		//access the last pointer
		NonLeafValue n = lastPointer.add(value,pNum,index);
		if(n==null)
			return null;
		Node temp = lastPointer;
		lastPointer= n.pointer;
		n.pointer = temp;
		values.add(n);
		Collections.sort(values);
		if(overflow()){
			NonLeaf x = new NonLeaf();
			int y = Bplus.n/2;
			x.lastPointer=this.lastPointer;
			this.lastPointer=values.get(y).pointer;
			NonLeafValue returned = new NonLeafValue(x,this.values.get(y).value);
			values.remove(y);
			for(int j = y;j<this.values.size();){
				x.values.add(this.values.get(j));
				this.values.remove(j);
			}
			return returned;
		}
		else return null;
	}
	
	public String toString(){
		return ""+this.values;
	}
	
	public LeafValue find(Comparable x){
		for(int i=0;i<values.size();i++){
			if((values.get(i).value).compareTo(x)>0)
				return values.get(i).pointer.find(x);
		}
		return lastPointer.find(x);
	}
}
