import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class TextIntPair implements WritableComparable<TextIntPair> {

	private Text left;
	private IntWritable right;

	public TextIntPair() {
		left = new Text();
		right = new IntWritable();
	}

	public TextIntPair(String first, int second) {
		super();
		set(first, second);
	}

	public void set(String first, int second) {
		left.set(first);
		right.set(second);
	}

	public String getLeft() {
		return left.toString();
	}

	public int getRight() {
		return right.get();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		left.write(out);
		right.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		left.readFields(in);
		right.readFields(in);
	}

	@Override
	public int hashCode() {
		return left.hashCode() * 163 + right.hashCode();
	}

	@Override
	public boolean equals(Object o) {
		if (o instanceof TextIntPair) {
			TextIntPair tp = (TextIntPair) o;
			return left.equals(tp.left) && right.equals(tp.right);
		}
		return false;
	}

	@Override
	public String toString() {
		return "[" + left + "\t" + right + "]";
	}

	@Override
	public int compareTo(TextIntPair tp) {
		int cmp = left.compareTo(tp.left);
		if (cmp != 0) {
			return cmp;
		}
		return right.compareTo(tp.right);
	}
}