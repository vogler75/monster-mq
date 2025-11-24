package at.rocworks.oa4j.var;

/**
 * Created by vogler on 6/6/17.
 */
public class NullVar extends Variable {
    public static NullVar NULL = new NullVar();

    @Override
    public String formatValue() {
        return "";
    }

    @Override
    public VariableType isA() {
        return VariableType.NullVar;
    }

    @Override
    public Object getValueObject() {
        return null;
    }
}
