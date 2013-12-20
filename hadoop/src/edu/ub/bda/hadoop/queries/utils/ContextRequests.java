package edu.ub.bda.hadoop.queries.utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;

/**
 *
 * @author olopezsa13
 */
public class ContextRequests implements WritableComparable<ContextRequests>
{

    protected String context;
    protected Long requests;
    
    public ContextRequests()
    {
    }

    public ContextRequests(String c, Long dt)
    {
        context = c;
        requests = dt;
    }

    public String getContext()
    {
        return context;
    }

    public Long getRequests()
    {
        return requests;
    }

    @Override
    public void write(DataOutput out) throws IOException
    {
        out.writeUTF(context);
        out.writeUTF(requests.toString());
    }

    @Override
    public void readFields(DataInput in) throws IOException
    {
        context = in.readUTF();
        requests = Long.parseLong(in.readUTF());
    }

    @Override
    public int compareTo(ContextRequests ct)
    {
        int cmp = this.context.compareTo(ct.context);
        if ( cmp != 0 )
        {
            return cmp;
        }

        return this.requests.compareTo(ct.requests);
    }

}
