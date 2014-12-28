package Basic;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
/**
* desc:Custom Data TOpes <code>Point</code>
* 
* @author chenwq
*/
	public class RdfWritable extends RdfNode implements WritableComparable {
		public String S;
		public String P;
		public String O;
		public String ts;

		public RdfWritable(String S, String P, String O,String ts) {
			this.S = S;
			this.P = P;
			this.O = O;
			this.ts = ts;
		}

		public RdfWritable() {
			this(null, null,null, null);
		}

		public void set(String S, String P, String O,String ts) {
			this.S = S;
			this.P = P;
			this.O = O;
			this.ts = ts;
		}

		public void write(DataOutput out) throws IOException {
			out.writeChars(S);
			out.writeChars(P);
			out.writeChars(O);
			out.writeChars(ts);
		}

		public void readFields(DataInput in) throws IOException {
			S = in.readLine();
			P = in.readLine();
			O = in.readLine();
			ts = in.readLine();
		}


		public boolean equals(Object o) {
			RdfWritable other = (RdfWritable) o;
			if (!(other instanceof RdfWritable)) {
				return false;
			}

			return this.S == other.S && this.P == other.P && this.O == other.O&&this.ts==other.ts;
		}

		@Override
		public int compareTo(Object o) {
			// TODO Auto-generated method stub
			return 0;
		}

	}
