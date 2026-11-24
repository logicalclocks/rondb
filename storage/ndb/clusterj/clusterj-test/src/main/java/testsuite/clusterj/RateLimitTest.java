/*
   Copyright (c) 2026, Hopsworks and/or its affiliates.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is designed to work with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL hereby grant you an additional
   permission to link the program and your derivative works with the
   separately licensed software that they have either included with
   the program or referenced in the documentation.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
*/

package testsuite.clusterj;

import com.mysql.clusterj.ClusterJDatastoreException;
import com.mysql.clusterj.ClusterJHelper;
import com.mysql.clusterj.ClusterJUserException;
import com.mysql.clusterj.Constants;
import com.mysql.clusterj.Session;
import com.mysql.clusterj.SessionFactory;

import java.util.Properties;

import org.junit.NamedOnly;

import testsuite.clusterj.model.RateLimitRow;

/**
 * RONDB-1158: a session charges the transactions it begins to a RonDB rate
 * limit USER. The ndb_quota MTR test ndb_clusterj_rate_limit runs it by
 * name after creating table rl_cj, USER rl_cj_user at a rate of 1 per
 * second, its alias rl_cj_alias and a rate quota on database test that
 * leaves untagged reads unthrottled; AllTests runs it only by name.
 */
@NamedOnly("run by ndb_quota.ndb_clusterj_rate_limit")
public class RateLimitTest extends AbstractClusterJTest {

    static final int ROWS = 10;
    static final int READS = 300;
    /** RonDB refuses a throttled write with 243 and a read with 2203. */
    static final int THROTTLED_WRITE = 243;
    static final int THROTTLED_READ = 2203;

    @Override
    public void localSetUp() {
        loadProperties();
        sessionFactory = ClusterJHelper.getSessionFactory(props);
        session = sessionFactory.getSession();
        for (int i = 0; i < ROWS; i++) {
            RateLimitRow row = session.newInstance(RateLimitRow.class);
            row.setId(i);
            row.setVal(i);
            session.savePersistent(row);
        }
    }

    /** Whether a failure is a throttling refusal; any other fails the test. */
    private boolean throttled(Exception ex) {
        for (Throwable t = ex; t != null; t = t.getCause()) {
            if (t instanceof ClusterJDatastoreException) {
                int code = ((ClusterJDatastoreException)t).getCode();
                if (code == THROTTLED_READ || code == THROTTLED_WRITE) {
                    return true;
                }
            }
        }
        error("Not a throttling refusal: " + ex);
        return false;
    }

    /** Reads in one autocommit transaction each; returns the throttled. */
    private int autocommitReads(Session s) {
        int count = 0;
        for (int i = 0; i < READS; i++) {
            try {
                s.find(RateLimitRow.class, i % ROWS);
            } catch (RuntimeException ex) {
                if (throttled(ex)) {
                    count++;
                }
            }
        }
        return count;
    }

    /** Reads in one explicit transaction each; returns the throttled. */
    private int explicitReads(Session s) {
        int count = 0;
        for (int i = 0; i < READS; i++) {
            try {
                s.currentTransaction().begin();
                s.find(RateLimitRow.class, i % ROWS);
                s.currentTransaction().commit();
            } catch (RuntimeException ex) {
                if (throttled(ex)) {
                    count++;
                }
                if (s.currentTransaction().isActive()) {
                    s.currentTransaction().rollback();
                }
            }
        }
        return count;
    }

    private static String repeat(char c, int n) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < n; i++) {
            builder.append(c);
        }
        return builder.toString();
    }

    private void refused(Session s, String name, String what) {
        try {
            s.setRateLimitUser(name);
            error(what + " accepted");
        } catch (ClusterJUserException ex) {
            // expected
        }
    }

    public void testNameLimits() {
        String name126 = repeat('a', 126);
        session.setRateLimitUser(name126);
        errorIfNotEqual("126 bytes", name126, session.getRateLimitUser());
        refused(session, repeat('a', 127), "127 bytes");
        refused(session, repeat('a', 128), "128 bytes");
        refused(session, repeat('a', 129), "129 bytes");
        errorIfNotEqual("a refused name keeps the one before",
                        name126, session.getRateLimitUser());
        // The limit counts UTF-8 bytes: 63 two-byte characters (a-ring, U+00E5) fit, 64 not. Escaped, since the
        // release build's javac runs under the POSIX locale and reads the source as US-ASCII.
        session.setRateLimitUser(repeat('\u00e5', 63));
        refused(session, repeat('\u00e5', 64), "128 bytes in 64 characters");
        refused(session, "", "an empty name");
        session.setRateLimitUser(null);
        errorIfNotEqual("null clears the name", null, session.getRateLimitUser());
        failOnError();
    }

    public void testUntaggedIsNotThrottled() {
        errorIfNotEqual("throttled untagged reads", 0, autocommitReads(session));
        failOnError();
    }

    public void testTaggedIsThrottled() {
        session.setRateLimitUser("rl_cj_user");
        if (autocommitReads(session) == 0) {
            error("no autocommit read was throttled");
        }
        if (explicitReads(session) == 0) {
            error("no read in an explicit transaction was throttled");
        }
        failOnError();
    }

    public void testAliasChargesItsTarget() {
        session.setRateLimitUser("rl_cj_alias");
        if (autocommitReads(session) == 0) {
            error("no read under the alias was throttled");
        }
        failOnError();
    }

    public void testUnknownNameIsNotThrottled() {
        session.setRateLimitUser("rl_cj_nobody");
        errorIfNotEqual("throttled reads under an unknown name",
                        0, autocommitReads(session));
        failOnError();
    }

    /** A transaction carries the name in effect when it began. */
    public void testNameIsTakenAtBegin() {
        session.setRateLimitUser("rl_cj_nobody");
        session.currentTransaction().begin();
        session.setRateLimitUser("rl_cj_user");
        int count = 0;
        try {
            for (int i = 0; i < READS; i++) {
                session.find(RateLimitRow.class, i % ROWS);
            }
            session.currentTransaction().commit();
        } catch (RuntimeException ex) {
            if (throttled(ex)) {
                count++;
            }
            if (session.currentTransaction().isActive()) {
                session.currentTransaction().rollback();
            }
        }
        errorIfNotEqual("throttled reads begun as rl_cj_nobody", 0, count);
        if (autocommitReads(session) == 0) {
            error("the transactions begun after it were not charged");
        }
        failOnError();
    }

    /**
     * A session handed back to the session cache with a transaction open
     * rolls it back and loses its name, so the next borrower starts clean.
     * The cache needs a factory of its own: pooled factories are shared
     * per connect string and database.
     */
    public void testCloseCacheRollsBackAndClearsTheName() {
        Properties cached = new Properties();
        cached.putAll(props);
        cached.setProperty(Constants.PROPERTY_CLUSTER_MAX_CACHED_SESSIONS, "1");
        cached.setProperty(Constants.PROPERTY_CONNECTION_POOL_SIZE, "0");
        SessionFactory factory = ClusterJHelper.getSessionFactory(cached);
        try {
            Session first = factory.getSession();
            first.setRateLimitUser("rl_cj_nobody");
            first.currentTransaction().begin();
            RateLimitRow row = first.newInstance(RateLimitRow.class);
            row.setId(1000);
            row.setVal(1);
            first.persist(row);
            first.flush();
            first.closeCache();
            Session next = factory.getSession();
            errorIfNotEqual("the cached session is handed out again",
                            true, next == first);
            errorIfNotEqual("open transaction",
                            false, next.currentTransaction().isActive());
            errorIfNotEqual("rate limit name", null, next.getRateLimitUser());
            errorIfNotEqual("the write was rolled back",
                            null, next.find(RateLimitRow.class, 1000));
            next.close();
        } finally {
            factory.close();
        }
        failOnError();
    }
}
