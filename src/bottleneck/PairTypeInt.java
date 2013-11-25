package bottleneck;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class PairTypeInt implements WritableComparable<PairTypeInt> {
	protected int pointA = -1;
	protected int pointB = -1;
	protected int pointC = -1;
	public int getA() {
		return pointA;
	}

	public int getB() {
		return pointB;
	}
	public int getC() {
		return pointC;
	}
	public void setA(int a) {
		this.pointA=a;
	}

	public void setB(int b) {
		this.pointB=b;
	}
	public void setC(int c) {
		this.pointC=c;
	}
	public void set(int point1, int point2,int point3) {
		pointA = point1;
		pointB = point2;
		pointC = point3;
	}
	public PairTypeInt(){}
	public PairTypeInt(int point1,int point2,int point3)
	{
		pointA=point1;
		pointB=point2;
		pointC=point3;
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(pointA);
		out.writeInt(pointB);
		out.writeInt(pointC);
	}

	public void readFields(DataInput in) throws IOException {
		pointA = in.readInt();
		pointB = in.readInt();
		pointC = in.readInt();
	}

	public int hashCode() {
		return Integer.rotateLeft(String.valueOf(pointA * 13 + pointC)
				.hashCode(), 7);
	}
	/*此处没用
	public int getPointnotV(int v)
	{
		if(this.getA()!=v)
			return this.getA();
		else
			return this.getB();
	}
	 */
	@Override
	public boolean equals(Object pair) {
		if (pair instanceof PairTypeInt) {
			PairTypeInt p = (PairTypeInt) pair;
			//return p.pointA == pointA && p.pointB == pointB && p.pointC == pointC;
			return p.pointA == pointA && p.pointC == pointC;
		} else {
			return false;
		}
	}

	/** A Comparator that compares serialized PairType. */
	public static class Comparator extends WritableComparator {
		public Comparator() {
			super(PairTypeInt.class);
		}

		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			return compareBytes(b1, s1, l1, b2, s2, l2);
		}
	}
/**
	static { // register this comparator
		WritableComparator.define(PairTypeInt.class, new Comparator());
	}
*/
	public int compareTo(PairTypeInt p) {
		if (pointA != p.pointA) {
			return pointA < p.pointA ? -1 : 1;
		}else if (pointC != p.pointC) {
			return pointC < p.pointC ? -1 : 1;
		}else {
			return 0;
		}
	}

	@Override
	public String toString() {
		return	"("+this.getA()+","+this.getB()+","+this.getC()+")";
	}
	
}
