/*
    OA4J - WinCC Open Architecture for Java
    Copyright (C) 2017 Andreas Vogler

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <https://www.gnu.org/licenses/>.
*/
package at.rocworks.oa4j.base;

import at.rocworks.oa4j.var.*;
//import javafx.util.Pair;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Static utility class providing the main API for WinCC OA datapoint operations.
 * <p>
 * JClient provides a fluent API for reading, writing, and subscribing to WinCC OA
 * datapoints. All methods are static and thread-safe. The manager must be initialized
 * via {@link JManager#init(String[])} before using JClient methods.
 * </p>
 *
 * <h3>Reading datapoints:</h3>
 * <pre>{@code
 * // Simple read
 * Variable value = JClient.dpGet("System1:ExampleDP_Arg1.");
 *
 * // Fluent API for multiple reads
 * JDpMsgAnswer answer = JClient.dpGet()
 *     .add("ExampleDP_Arg1.")
 *     .add("ExampleDP_Arg2.")
 *     .await();
 * }</pre>
 *
 * <h3>Writing datapoints:</h3>
 * <pre>{@code
 * // Simple write (fire and forget)
 * JClient.dpSet("ExampleDP_Arg1.", 42);
 *
 * // Write and wait for confirmation
 * int retCode = JClient.dpSetWait("ExampleDP_Arg1.", 42);
 *
 * // Fluent API for multiple writes
 * JClient.dpSet()
 *     .add("ExampleDP_Arg1.", 100)
 *     .add("ExampleDP_Arg2.", "text")
 *     .send();
 * }</pre>
 *
 * <h3>Subscribing to changes (hotlink):</h3>
 * <pre>{@code
 * JClient.dpConnect()
 *     .add("ExampleDP_Arg1.")
 *     .action((JDpHLGroup hlg) -> {
 *         hlg.forEach(item -> System.out.println(item.getVariable()));
 *     })
 *     .connect();
 * }</pre>
 *
 * <h3>Querying datapoints:</h3>
 * <pre>{@code
 * JDpMsgAnswer answer = JClient.dpQuery("SELECT '_online.._value' FROM 'ExampleDP_*'").await();
 * }</pre>
 *
 * @author vogler
 * @see JManager
 * @see JDpGet
 * @see JDpSet
 * @see JDpConnect
 * @see JDpQuery
 */
public class JClient {

    /**
     * Checks if the manager is connected to WinCC OA.
     *
     * @return true if the manager is initialized and connected
     */
    public static boolean isConnected() {
        return JManager.getInstance()!=null && JManager.getInstance().isConnected();
    }

    // ========== dpGet ==========

    /**
     * Creates a new datapoint get request builder.
     * <p>
     * Use the fluent API to add datapoints and execute the request.
     * </p>
     *
     * <pre>{@code
     * JDpMsgAnswer answer = JClient.dpGet()
     *     .add("dp1.")
     *     .add("dp2.")
     *     .await();
     * }</pre>
     *
     * @return a new JDpGet builder instance
     */
    public static JDpGet dpGet() {
        return (new JDpGet());
    }

    /**
     * Reads a single datapoint value synchronously.
     *
     * @param dp the datapoint name (e.g., "System1:ExampleDP_Arg1.")
     * @return the datapoint value, or null if not found or error
     */
    public static Variable dpGet(String dp) {
        JDpMsgAnswer answer = (new JDpGet()).add(dp).await();
        return ( answer.size()>0 ) ? answer.getItem(0).getVariable() : null;
    }

    /**
     * Reads a single datapoint value into a variable pointer.
     *
     * @param dp  the datapoint name
     * @param var output parameter that receives the value
     * @return return code (0 = success)
     */
    public static int dpGet(String dp, VariablePtr var) {
        JDpMsgAnswer answer = (new JDpGet()).add(dp).await();
        if ( answer.size()>0 ) var.set(answer.getItem(0).getVariable());
        return answer.getRetCode();
    }

    /**
     * Reads multiple datapoint values synchronously.
     *
     * @param dps list of datapoint names to read
     * @return list of values in the same order as the input
     */
    public static List<Variable> dpGet(List<String> dps) {
        JDpGet dpGet = new JDpGet();
        dps.forEach((dp)->dpGet.add(dp));
        JDpMsgAnswer answer = dpGet.await();
        List<Variable> ret = new ArrayList<Variable>(answer.size());
        answer.forEach((item)->ret.add(item.getVariable()));
        return ret;
    }

    // ========== dpGetPeriod ==========

    /**
     * Creates a historical data query for a time period.
     * <p>
     * Retrieves archived values between start and stop times.
     * </p>
     *
     * @param start start time of the period
     * @param stop  end time of the period
     * @param num   maximum number of values to retrieve (0 = unlimited)
     * @return a new JDpGetPeriod builder instance
     */
    public static JDpGetPeriod dpGetPeriod(TimeVar start, TimeVar stop, int num) {
        return (new JDpGetPeriod(start, stop, num));
    }

    /**
     * Creates a historical data query using Java Date objects.
     *
     * @param start start time as Date
     * @param stop  end time as Date
     * @param num   maximum number of values (0 = unlimited)
     * @return a new JDpGetPeriod builder instance
     */
    public static JDpGetPeriod dpGetPeriod(Date start, Date stop, int num) {
        return dpGetPeriod(new TimeVar(start), new TimeVar(stop), num);
    }

    /**
     * Creates a historical data query using millisecond timestamps.
     *
     * @param start start time in milliseconds since epoch
     * @param stop  end time in milliseconds since epoch
     * @param num   maximum number of values (0 = unlimited)
     * @return a new JDpGetPeriod builder instance
     */
    public static JDpGetPeriod dpGetPeriod(Long start, long stop, int num) {
        return dpGetPeriod(new TimeVar(start), new TimeVar(stop), num);
    }

    // ========== dpSet ==========

    /**
     * Creates a new datapoint set request builder.
     * <p>
     * Use the fluent API to add datapoint/value pairs and execute.
     * </p>
     *
     * <pre>{@code
     * JClient.dpSet()
     *     .add("dp1.", 100)
     *     .add("dp2.", "text")
     *     .send();
     * }</pre>
     *
     * @return a new JDpSet builder instance
     */
    public static JDpSet dpSet() {
        return (new JDpSet());
    }

    /**
     * Writes a single datapoint value asynchronously (fire and forget).
     *
     * @param dp  the datapoint name
     * @param var the value to write (automatically converted to Variable)
     * @return the JDpSet instance (already sent)
     */
    public static JDpSet dpSet(String dp, Object var) {
        return (new JDpSet()).add(dp, Variable.newVariable(var)).send();
    }

    /**
     * Writes a single datapoint value synchronously and waits for confirmation.
     *
     * @param dp  the datapoint name
     * @param var the value to write
     * @return return code (0 = success)
     */
    public static int dpSetWait(String dp, Object var) {
        return dpSet(dp, var).await().getRetCode();
    }

    // ========== dpConnect ==========

    /**
     * Creates a new datapoint connection (hotlink) builder.
     * <p>
     * Hotlinks provide real-time notifications when datapoint values change.
     * </p>
     *
     * <pre>{@code
     * JClient.dpConnect()
     *     .add("ExampleDP_Arg1.")
     *     .action(hlg -> {
     *         hlg.forEach(item -> {
     *             System.out.println(item.getDpName() + " = " + item.getVariable());
     *         });
     *     })
     *     .connect();
     * }</pre>
     *
     * @return a new JDpConnect builder instance
     */
    public static JDpConnect dpConnect() {
        return (new JDpConnect());
    }

    // ========== alertConnect ==========

    /**
     * Creates a new alert connection builder.
     * <p>
     * Subscribes to alarm/alert notifications from WinCC OA.
     * </p>
     *
     * @return a new JAlertConnect builder instance
     */
    public static JAlertConnect alertConnect() {
        return (new JAlertConnect());
    }

    // ========== dpQuery ==========

    /**
     * Executes a datapoint query using WinCC OA SQL-like syntax.
     * <p>
     * The query is sent immediately. Call {@code .await()} to get results.
     * </p>
     *
     * <pre>{@code
     * JDpMsgAnswer answer = JClient.dpQuery(
     *     "SELECT '_online.._value' FROM 'ExampleDP_*'"
     * ).await();
     * }</pre>
     *
     * @param query the query string in WinCC OA query syntax
     * @return a JDpQuery instance (already sent)
     */
    public static JDpQuery dpQuery(String query) {
        return (new JDpQuery(query).send());
    }

    // ========== dpQueryConnect ==========

    /**
     * Creates a query-based hotlink that triggers only on the first matching change.
     *
     * @param query the query string
     * @return a new JDpQueryConnectSingle builder instance
     */
    public static JDpQueryConnect dpQueryConnectSingle(String query) {
        return (new JDpQueryConnectSingle(query));
    }

    /**
     * Creates a query-based hotlink that triggers on all matching changes.
     *
     * @param query the query string
     * @return a new JDpQueryConnectAll builder instance
     */
    public static JDpQueryConnect dpQueryConnectAll(String query) {
        return (new JDpQueryConnectAll(query));
    }

    // ========== dpNames ==========

    /**
     * Returns datapoint names matching a pattern.
     *
     * @param pattern wildcard pattern (e.g., "ExampleDP_*")
     * @return array of matching datapoint names
     */
    public static String[] dpNames(String pattern) {
        return (String[])JManager.getInstance().executeTask(()->{
            return JManager.getInstance().apiGetIdSet(pattern);
        });
    }

    /**
     * Returns datapoint names matching a pattern and type.
     *
     * @param pattern wildcard pattern (e.g., "ExampleDP_*")
     * @param type    datapoint type name to filter by
     * @return array of matching datapoint names
     */
    public static String[] dpNames(String pattern, String type) {
        return (String[])JManager.getInstance().executeTask(()->{
            return JManager.getInstance().apiGetIdSetOfType(pattern, type);
        });
    }

    // ========== dpComment ==========

    /**
     * Retrieves the comment/description for a datapoint.
     *
     * @param dpid the datapoint identifier
     * @return the language-dependent comment text
     */
    public static LangTextVar dpGetComment(DpIdentifierVar dpid) {
        return (LangTextVar)JManager.getInstance().executeTask(()->{
            return JManager.getInstance().apiDpGetComment(dpid);
        });
    }

    /**
     * Verfiy password. Check if the given passwd is valid for the requested user id
     * @param username
     * @param password
     * @return 0...Ok, -1...invalid user, -2...wrong password
     */
    public static int checkPassword(String username, String password) {
        return (int)JManager.getInstance().executeTask(()->{
            return JManager.getInstance().checkPassword(username, password);
        });
    }

    /**
     * A new user id is set when (id matches passwd) or
     * (currentId is ROOT_USER and newUserId exists) or
     * (newUserId is DEFAULT_USER).
     * @param username
     * @param password
     * @return true if user has been set
     */
    public static boolean setUserId(String username, String password) {
        return (boolean)JManager.getInstance().executeTask(()->{
            return JManager.getInstance().setUserId(username, password);
        });
    }
}
