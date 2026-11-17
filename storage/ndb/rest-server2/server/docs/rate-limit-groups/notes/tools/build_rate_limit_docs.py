import re, sys
D='<repos>/rondb/storage/ndb/rest-server2/server/docs/'
old=open(D+'RonDB-Group-Rate-Limits.html',encoding='utf-8').read()

def sec(i):
    m=re.search(r'<section id="%s">.*?</section>\n'%i, old, re.S); assert m, i; return m.group(0)
def must(s, a, b, count=1):
    assert s.count(a)==count, (a[:60], s.count(a)); return s.replace(a,b)

head=old[:old.index('<body>')]
# ---------------------------------------------------------------- DESIGN DOC
SECMAP={6:8,7:9,8:10,9:11,10:12,11:13,12:15}
def renum(s):
    s=re.sub(r'<h2>(\d+)\. ', lambda m:'<h2>%d. '%SECMAP.get(int(m.group(1)),int(m.group(1))), s)
    s=re.sub(r'\b([Ss]ection) (\d+)\b', lambda m:'%s %d'%(m.group(1),SECMAP.get(int(m.group(2)),int(m.group(2)))), s)
    return s

header=old[old.index('<header>'):old.index('</header>')+len('</header>')]
header=must(header,'<p class="eyebrow">RonDB · Design proposal · October 2026</p>','<p class="eyebrow">RonDB · Design proposal · October 2026 · revision 2</p>')
header=must(header,'<h1>Rate-limit buckets: shared database budgets for teams</h1>','<h1>Rate-limit buckets: design</h1>')
header=must(header,"""database tables. The RonDB REST server (RDRS) reads those tables, works out which bucket
  each incoming request belongs to, and tags the request with the bucket’s name. RonDB
  then enforces the budget using the rate-limit feature it already has.</p>""",
"""database tables. The RonDB REST server (RDRS) reads those tables and installs them on the
  cluster. Every request, whether it arrives through RDRS with an API key or through the
  MySQL server as a Spark job, notebook or dashboard, is tagged with the caller’s identity,
  and RonDB works out which bucket that identity and that table belong to. RonDB then
  enforces the budget using the rate-limit feature it already has.</p>""")
header=must(header,"""cluster. It shows the exact rows that get stored, how an API key is mapped to a bucket,
  and how the limits reach the cluster. Deployments with several RonDB clusters are
  covered in <a href="#multi">section 9</a>.</p>""",
"""cluster. It shows the exact rows that get stored, how an API key is mapped to a bucket
  (<a href="#mapping">section 5</a>), how a SQL session is mapped to the same bucket
  (<a href="#sqlpath">section 6</a>), what that needs from RonDB
  (<a href="#kernel">section 7</a>), and how the limits reach the cluster. Deployments
  with several RonDB clusters are covered in <a href="#multi">section 11</a>. A shorter
  companion, <em>Rate-limit buckets: a shared database budget for every team</em>, gives
  the big picture for customers.</p>""")
header=must(header,"""described here (the tables, the admin page, the RDRS lookup, and the sync to the cluster)
  is new work. No existing Hopsworks table is changed.</p>""",
"""described here (the tables, the admin page, the sync to the cluster, and the alias object
  in RonDB) is new work. No existing Hopsworks table is changed. Facts about existing code
  were verified on 2026-10-21 against Hopsworks <code>83963e2354</code> and RonDB
  26.02-main.</p>""")

nav="""<nav aria-label="Contents">
  <ol>
    <li><a href="#problem">The problem and the idea</a></li>
    <li><a href="#teams">The teams, and what Hopsworks already stores about them</a></li>
    <li><a href="#tables">The new tables</a></li>
    <li><a href="#example">The example: one cluster, three wishes</a></li>
    <li><a href="#mapping">How an API key ends up in a bucket</a></li>
    <li><a href="#sqlpath">How a SQL session ends up in the same bucket</a></li>
    <li><a href="#kernel">What changes in RonDB: the alias</a></li>
    <li><a href="#rules">Rules when saving and deleting</a></li>
    <li><a href="#sync">Getting the limits onto the cluster</a></li>
    <li><a href="#admin">The admin page</a></li>
    <li><a href="#multi">Later: more than one cluster</a></li>
    <li><a href="#build">What needs to be built</a></li>
    <li><a href="#mechanics">Reference: how RonDB enforces a limit</a></li>
    <li><a href="#open">Open decisions</a></li>
    <li><a href="#ddl">Appendix: proposed table definitions and commands</a></li>
  </ol>
</nav>
"""

s1=sec('problem'); s2=sec('teams'); s3=sec('tables'); s4=sec('example'); s5=sec('mapping')
s6=renum(sec('rules')); s7=renum(sec('sync')); s8=renum(sec('admin')); s9=renum(sec('multi'))
s10=renum(sec('build')); s11=renum(sec('mechanics')); s12=renum(sec('ddl'))
s1,s2,s3,s4,s5=map(renum,(s1,s2,s3,s4,s5))

# §1: one sentence on SQL
s1=must(s1,"""<li>Give each key 7%? Alice’s training job hits its 7% while the other two keys sit idle.</li>
  </ul>""","""<li>Give each key 7%? Alice’s training job hits its 7% while the other two keys sit idle.</li>
    <li>And Ava’s Spark job reads the same tables through the MySQL server, where there is
    no API key at all.</li>
  </ul>""")
s1=must(s1,"""When Alice reads a table, RDRS finds the bucket that contains both Alice and that table,
and tags the transaction with the bucket’s name.""","""When Alice reads a table, the bucket that contains both Alice and that table is found,
and her work is charged to it, whether she came through RDRS or through the MySQL server.""")

# §3.2: airflow fact
s3=must(s3,"""a decision taken on whether to exclude them.</li>""","""a decision taken on whether to exclude them. One correction to the list: the
      <code>airflow</code> user is on every team but is filtered out before MySQL accounts are
      created (<code>ProjectUtils.java:376</code>), so it has no account and no SQL traffic;
      <code>onlinefs</code> and <code>srvmanager</code> do have accounts.</li>""")

# §5 heading tweak: keep. Add forward pointer at the end of edge cases? keep as is.

# ---------------- NEW §6
s_sql = """<section id="sqlpath">
<h2>6. How a SQL session ends up in the same bucket</h2>
<p><a href="#mapping">Section 5</a> covers requests that arrive at RDRS with an API key.
A large share of feature-store traffic never touches RDRS: Spark and Flink jobs,
notebooks, dashboards and the backend’s own preview queries go to the MySQL server. For a
budget to mean anything, that traffic has to land in the same bucket. This section shows
that it can, and what it takes.</p>
<div class="tablewrap"><table>
  <thead><tr><th scope="col">How the team connects</th><th scope="col">Goes through</th><th scope="col">Identified by</th></tr></thead>
  <tbody>
    <tr><td>Python / Java feature-store client, online reads (<code>get_feature_vector</code> REST path)</td><td>RDRS</td><td>API key → its owner (section 5)</td></tr>
    <tr><td>Spark or Flink jobs, Jupyter, the hsfs SQL engine, Superset, any SQL tool</td><td>MySQL server</td><td>the member account Hopsworks created for the caller in that project</td></tr>
    <tr><td>Model-serving pods</td><td>both: RDRS with a key minted for the person who started the deployment, MySQL with that person’s account (<code>MYSQL_USER</code> env)</td><td>same person</td></tr>
    <tr><td>Hopsworks backend: online preview, feature-group DDL</td><td>MySQL server</td><td>the caller’s own account</td></tr>
    <tr><td>Hopsworks backend: schema reads, grants, tracing, ingestion results</td><td>MySQL server</td><td>the admin account (never metered)</td></tr>
    <tr><td>Online ingestion (<code>onlinefs</code>)</td><td>ClusterJ, straight to the data nodes</td><td>nothing today</td></tr>
  </tbody>
</table></div>

<h3>6.1 The account the MySQL server sees</h3>
<p>Hopsworks creates one MySQL account per project member, when the project’s online
feature store is first used and again whenever a member is added
(<code>OnlineFeaturestoreController.createDatabaseUser</code>). The name is the project
name, an underscore, and the login user’s <code>username</code>; if that is longer than
32 characters it is cut to 31 (<code>onlineDbUsername</code>, lines 589-595). The host
part is <code>'%'</code>. The data owner gets <code>ALL</code> on the project’s database,
the data scientist <code>SELECT</code>; a share grants <code>SELECT</code> on the owner’s
database or table to the <em>receiving</em> project’s member accounts.</p>
<div class="tablewrap"><table class="rows">
  <caption>Accounts for the example cast <span>(usernames are the generated 8-character handles; illustrative)</span></caption>
  <thead><tr><th scope="col">Person</th><th scope="col">users.username</th><th scope="col">Project</th><th scope="col">MySQL account</th><th scope="col">Grants</th></tr></thead>
  <tbody>
    <tr><td>Mary (1001)</td><td><code>mary01ab</code></td><td>Marketing (11)</td><td><code>Marketing_mary01ab</code></td><td>ALL on <code>marketing.*</code></td></tr>
    <tr><td>Mike (1002)</td><td><code>mike7c3d</code></td><td>Marketing (11)</td><td><code>Marketing_mike7c3d</code></td><td>ALL on <code>marketing.*</code></td></tr>
    <tr><td>Sam (1003)</td><td><code>sam00035</code></td><td>Sales (12)</td><td><code>Sales_sam00035</code></td><td>ALL on <code>sales.*</code></td></tr>
    <tr><td>Alice (1004)</td><td><code>alice4f2</code></td><td>Recommender (13)</td><td><code>Recommender_alice4f2</code></td><td>ALL on <code>recommender.*</code>; SELECT on <code>marketing.*</code>; SELECT on <code>sales.sales_orders_1</code></td></tr>
    <tr><td>Ava (1005)</td><td><code>ava3b7c0</code></td><td>Recommender (13)</td><td><code>Recommender_ava3b7c0</code></td><td>same as Alice</td></tr>
    <tr><td>Rosa (1006)</td><td><code>rosa9e1f</code></td><td>Recommender2 (14)</td><td><code>Recommender2_rosa9e1f</code></td><td>ALL on <code>recommender2.*</code>; SELECT on <code>marketing.*</code></td></tr>
  </tbody>
</table></div>
<p>Two facts that matter for buckets. A reader of shared data always connects as their
<em>own</em> project’s account: Alice reads Marketing’s store as
<code>Recommender_alice4f2</code>, and the owning project never appears in the name. And
the name is <strong>the same string</strong> that RDRS’s username mode produces for her
(<code>RONDB-978-Cont</code> builds it with the same clip), so one person is one name on
both doors.</p>

<h3>6.2 What the MySQL server already does</h3>
<p>Since RonDB 26.02 the NDB handler tags every NDB transaction with the session’s login
name (<code>ha_ndbcluster::set_user_id</code>, <code>ha_ndbcluster.cc:7756</code>, called
right after <code>startTransaction</code> in all four places a transaction is started).
Three properties of that tagging shape the design:</p>
<ul>
  <li><strong>The name, not the host.</strong> It is
  <code>security_context()-&gt;user()</code>, so <code>Marketing_mary01ab</code>, exactly
  the string Hopsworks used in <code>CREATE USER</code>. Inside a routine declared
  <code>SQL SECURITY DEFINER</code> it is the definer’s name.</li>
  <li><strong>Once per transaction, before any table is known.</strong> All tables of a
  session share one <code>Thd_ndb::trans</code>; a <code>JOIN</code>, or a
  <code>BEGIN … COMMIT</code> block, carries one tag chosen when the first handler starts
  the transaction. Nothing re-tags per table.</li>
  <li><strong>Off by default.</strong> The read-only server variable
  <code>ndb_user_rate_limits</code> (default OFF) and the data-node setting
  <code>ActivateRateLimits</code> (default false) must both be on. Internal work (binlog
  thread, schema distribution, stored-grant replication) is never tagged.</li>
</ul>

<h3>6.3 From account name back to a person</h3>
<p>Nothing in Hopsworks stores “account → user” directly: the <code>secrets</code> row is
named after the account but carries only the uid, and <code>operation_log</code> rows are
a queue, not a registry. The name is still decodable without a table, because of three
rules Hopsworks enforces elsewhere:</p>
<ul>
  <li>Project names are at most 25 characters from <code>[A-Za-z0-9_]</code> with no
  double underscore (<code>FolderNameValidator.java:54-88</code>) and are unique ignoring
  case.</li>
  <li>Generated usernames are exactly eight lowercase alphanumerics
  (<code>UsersController.java:517-552</code>); service usernames (<code>onlinefs</code>,
  <code>srvmanager</code>) contain no underscore.</li>
  <li>The clip cuts the username, never the project name: a 25-character project leaves
  five characters of the username.</li>
</ul>
<div class="tablewrap"><table>
  <thead><tr><th scope="col">Account</th><th scope="col">Split at the last “_”</th><th scope="col">Resolves to</th></tr></thead>
  <tbody>
    <tr><td><code>Marketing_mary01ab</code></td><td><code>Marketing</code> + <code>mary01ab</code></td><td>project 11; uid 1001 exactly</td></tr>
    <tr><td><code>Recommender2_rosa9e1f</code></td><td><code>Recommender2</code> + <code>rosa9e1f</code></td><td>project 14; uid 1006 exactly</td></tr>
    <tr><td><code>CustomerLifetimeValue25_rosa9</code> (31 chars)</td><td><code>CustomerLifetimeValue25</code> + <code>rosa9</code></td><td>project by name; then the member of that project whose own clipped name equals the account</td></tr>
  </tbody>
</table></div>
<p>So the resolver recomputes clip31 over the project’s members and compares. In the
design below that happens once per member in the sync loop, never per request.</p>
<div class="note">
  <p><strong>Collisions exist today and are not solved here.</strong> Two members of a
  24- or 25-character project whose usernames share their first five or six characters
  clip to the same account. Hopsworks has no guard
  (<code>CREATE USER IF NOT EXISTS</code> silently reuses the account), the second
  member’s login already fails, and removing either member drops the shared account. For
  buckets this means such a pair shares one identity; it should be documented as a
  limitation of the account scheme, not fixed by the bucket design.</p>
</div>

<h3>6.4 Why the lookup of section 5 cannot run in the MySQL server</h3>
<p>Section 5 resolves (person, table) → bucket inside RDRS, per request, from two cached
maps. The MySQL server cannot do the same, for three reasons that follow from 6.2:</p>
<ol>
  <li>The tag is fixed per transaction before the tables are known, so a per-table
  decision would need the handler to defer or re-tag, and a <code>JOIN</code> across two
  tables in different buckets has no single right answer at the server.</li>
  <li>The server would need the bucket tables, the Hopsworks membership expansion and an
  event-driven cache of them, which is RDRS’s Hopsworks-metadata cache rebuilt inside
  mysqld. The closest existing template (the index-statistics listener on
  <code>ndb_index_stat_head</code>) shows it is possible, but it is a second copy of
  everything in section 5.</li>
  <li>ClusterJ, if ingestion is ever metered, would need a third copy.</li>
</ol>
<p>What every path <em>does</em> have in common is that it tags the transaction with a
name that identifies the caller, and that the data nodes see the table next to that name
at every point where they admit or charge work: <code>TCKEYREQ</code>
(<code>DbtcMain.cpp:3726</code>), <code>SCAN_TABREQ</code> (<code>:16175</code>),
<code>TCINDXREQ</code> (<code>:22396</code>), and the charging function itself,
<code>Dblqh::update_rate_usage(tableId, userPtrI, ns)</code>
(<code>DblqhMain.cpp:40517</code>). The kernel is the one place where (caller, table) →
bucket can be decided for all paths at once. What is missing is an object that says
“this name, on this data, charges that bucket”. That is the <strong>alias</strong>, and
<a href="#kernel">section 7</a> specifies it.</p>

<h3>6.5 Mary’s statement, step by step</h3>
<pre><code>mysql -u Marketing_mary01ab -p …
mysql&gt; SELECT * FROM marketing.marketing_clicks_1 WHERE click_id = 42;</code></pre>
<figure class="erd"><img src="img/kernel-resolution.png" alt="Dbtc resolves the tagged account name and the table id through the alias’s rules to the budgeted USER clicks_own; a second example resolves Alice’s account through an ALL rule to recommender."><figcaption>What the data node does with Mary’s tag and Alice’s. An alias has ordered rules (table, then database, then all); the first that matches names the bucket to charge. A plain <code>USER</code> has no rules and is charged directly.</figcaption></figure>
<div class="tablewrap"><table>
  <thead><tr><th scope="col">Step</th><th scope="col">What happens</th><th scope="col">Result</th><th scope="col">Status</th></tr></thead>
  <tbody>
    <tr><td>1</td><td>Mary logs in; the server’s security context holds <code>Marketing_mary01ab</code>.</td><td>account known</td><td>exists</td></tr>
    <tr><td>2</td><td>The handler starts an NDB transaction and calls <code>setUserId("Marketing_mary01ab")</code>.</td><td>tagged</td><td>exists</td></tr>
    <tr><td>3</td><td>The NDB API’s name cache (<code>ClusterMgr::retrieveUserId</code>) maps the name to a dictionary object id. Today only <code>USER</code> objects are in that cache; aliases join it.</td><td>object id of the alias</td><td>exists; aliases listed</td></tr>
    <tr><td>4</td><td><code>TCKEYREQ</code> arrives at Dbtc with that id and <code>tableId</code> 1042 (<code>marketing.marketing_clicks_1</code>).</td><td>id + table</td><td>exists</td></tr>
    <tr><td>5</td><td>The object is an alias. Rule 1 (<code>ON marketing.marketing_clicks_1</code>) matches table 1042.</td><td>bucket <code>clicks_own</code></td><td>new</td></tr>
    <tr><td>6</td><td>Dbtc records <code>clicks_own</code> as the transaction’s bucket (the sticky first record, <code>m_queuedDatabasePtrI</code>) and forwards it to Dblqh, which charges 1.25 µs plus data read.</td><td>charged</td><td>exists</td></tr>
  </tbody>
</table></div>
<p>Step 5 is the only new work on the request path, and only for names that are aliases.
Here is the same walk for other sessions:</p>
<div class="tablewrap"><table>
  <thead><tr><th scope="col">Session</th><th scope="col">Tag</th><th scope="col">Table</th><th scope="col">Alias rule that matches</th><th scope="col">Charged to</th></tr></thead>
  <tbody>
    <tr><td>Mary, SQL</td><td><code>Marketing_mary01ab</code></td><td><code>marketing.marketing_campaigns_1</code></td><td>none (rule 1 is the clicks table only)</td><td>no bucket; unmetered, as today</td></tr>
    <tr><td>Alice, Spark job</td><td><code>Recommender_alice4f2</code></td><td><code>sales.sales_orders_1</code></td><td>ALL</td><td><code>recommender</code></td></tr>
    <tr><td>Alice, REST request (section 5), RDRS in username mode</td><td><code>Recommender_alice4f2</code></td><td><code>marketing.marketing_clicks_1</code></td><td>ALL</td><td><code>recommender</code>, the same alias as her Spark job</td></tr>
    <tr><td>Rosa, Superset</td><td><code>Recommender2_rosa9e1f</code></td><td><code>marketing.marketing_campaigns_1</code></td><td><code>ON marketing</code></td><td><code>rec2_on_marketing</code></td></tr>
    <tr><td>Hopsworks backend, grants and schema reads</td><td>admin account</td><td>any</td><td>no alias</td><td>unmetered, by design</td></tr>
    <tr><td>Alice, SQL <code>JOIN</code> of <code>marketing_clicks_1</code> and <code>sales_orders_1</code></td><td><code>Recommender_alice4f2</code></td><td>both</td><td>ALL on the first table touched</td><td><code>recommender</code> for the whole transaction</td></tr>
  </tbody>
</table></div>

<h3>6.6 One mechanism for both doors</h3>
<p>Once RonDB resolves aliases, RDRS no longer needs to find the bucket itself. It tags
the transaction with the same project-user string the MySQL server would use
(username mode, already implemented on <code>RONDB-978-Cont</code> and byte-identical
to the account name), and the alias does the rest. The two maps of section 5 become an
optional first step rather than the mechanism. The whole flow, both the sync that creates
the objects and the request paths that use them:</p>
<figure class="erd"><img src="img/architecture.png" alt="Hopsworks tables feed an RDRS sync loop that installs USER and USER ALIAS objects through the management server; REST, SQL and ClusterJ requests all reach Dbtc admission, which resolves tag plus table to a bucket and hands it to Dblqh."><figcaption>Two loops. The sync loop (left) turns bucket rows into <code>USER</code> and <code>USER ALIAS</code> objects on every cluster. The request paths (right) tag transactions; Dbtc resolves tag plus table to a bucket; Dblqh charges it.</figcaption></figure>
<div class="tablewrap"><table>
  <thead><tr><th scope="col"></th><th scope="col">RDRS resolves (section 5 only)</th><th scope="col">RonDB resolves (section 5 + this section)</th></tr></thead>
  <tbody>
    <tr><td>Paths covered</td><td>REST</td><td>REST, SQL, ClusterJ</td></tr>
    <tr><td>Data-scoped buckets on SQL</td><td>no</td><td>yes</td></tr>
    <tr><td>RonDB change</td><td>none</td><td>alias object, Dbtc lookup, mgm commands (section 7)</td></tr>
    <tr><td>MySQL server change</td><td>none, and SQL is not covered</td><td>none</td></tr>
    <tr><td>RDRS change</td><td>cache + two maps + intersection per request + sync</td><td>sync (section 9); tag the project-user name</td></tr>
    <tr><td>Objects on the cluster</td><td>one <code>USER</code> per bucket</td><td>one <code>USER</code> per bucket, plus one alias per member account; aliases are not budgeted</td></tr>
    <tr><td>A membership change takes effect</td><td>at the next RDRS cache refresh</td><td>at the next sync run</td></tr>
    <tr><td>Per-request cost</td><td>two lookups and an intersection in RDRS</td><td>one rule lookup in Dbtc for aliased names; nothing for plain <code>USER</code>s</td></tr>
    <tr><td>A transaction over several tables</td><td>RDRS picks (feature view: the most restrictive bucket)</td><td>the first table touched decides, as the kernel does today for every transaction</td></tr>
  </tbody>
</table></div>
<p><strong>Proposal: RonDB resolves.</strong> The Hopsworks tables, the admin page and
the sync loop are identical in both columns, so shipping the left column first as a
REST-only step wastes little, but the right column is the design. Whether to ship the
left column at all is listed in <a href="#open">section 14</a>.</p>
</section>

"""

s_kernel = """<section id="kernel">
<h2>7. What changes in RonDB: the alias</h2>
<p>Everything in this section is new. It builds on four facts about the 26.02
implementation:</p>
<ul>
  <li>Names resolve on the client. <code>ClusterMgr::retrieveUserId</code>
  (<code>ClusterMgr.cpp:1512</code>) keeps a hash filled from a <code>LIST_DATABASE</code>
  walk with <code>is_user=1</code>, kept current by <code>CREATE/DROP_DATABASE_REP</code>
  and a full re-list every 60 seconds. Only a 32-bit object id and version go on the wire
  (<code>TcKeyReq</code> bit 29, <code>ScanTabReq</code> bit 5). An unknown name returns
  <code>RNIL</code> and the transaction runs unmetered.</li>
  <li>Dbdict stores a <code>USER</code> as <code>$name</code>, in the same namespace and
  id space as <code>DATABASE QUOTA</code> objects (<code>Dbdict.cpp:33182</code>); 127
  usable characters.</li>
  <li>Dbtc keeps the first record a transaction touches in
  <code>ApiConnectRecord::m_queuedDatabasePtrI</code> (<code>DbtcMain.cpp:28543</code>,
  <code>:16205</code>) and forwards the user id to Dblqh only when that record is a user
  (<code>LQHKEYREQ</code> bit 9, <code>SCAN_FRAGREQ</code> bit 27). Dbspj copies it into
  every child operation.</li>
  <li>Dbdict already binds each table to a quota database when the table is created
  (<code>CONNECT_TABLE_DB</code>, <code>Dbdict.cpp:8540</code>), so a “table → budget
  object” link is an existing pattern.</li>
</ul>

<h3>7.1 The object</h3>
<p>A <code>USER ALIAS</code> is a Dbdict object stored like a <code>USER</code>
(<code>$name</code>), flagged as an alias, with <strong>no budget</strong> and an ordered
list of rules. A rule is (scope, target): the scope is one table, one database, or
<em>all</em>; the target is a <code>USER</code>. Dbtc evaluates table rules first, then
database rules, then the all rule; the first match wins; no match means the transaction
runs unmetered, which is today’s unknown-name behavior.</p>
<div class="tablewrap"><table>
  <thead><tr><th scope="col">Property</th><th scope="col">Behavior</th></tr></thead>
  <tbody>
    <tr><td>Budget</td><td>None. Aliases are excluded from <code>m_current_allocated_rate</code> and never count toward the 50% cap (<a href="#mechanics">section 13</a>). A thousand aliases cost the pool nothing.</td></tr>
    <tr><td>Name cache</td><td>The <code>LIST_DATABASE</code> walk returns aliases alongside users, so mysqld, RDRS and ClusterJ resolve an alias name to its id with no NDB API change.</td></tr>
    <tr><td>Persistence</td><td>Stored in the schema file like quota objects, recreated on system restart, included in <code>USER BACKUP</code> / restore.</td></tr>
    <tr><td>Database rules</td><td>Dbtc needs the table’s database to match a database rule. Two options: Dbdict expands a database rule into that database’s tables and maintains it on <code>CREATE</code>/<code>DROP TABLE</code>; or the table record carries its database id for Dbtc to compare. Kernel team’s choice.</td></tr>
    <tr><td>Name rules</td><td>Same as <code>USER</code>: at most 127 characters, exact case-sensitive match. An alias and a user may not share a name.</td></tr>
  </tbody>
</table></div>

<h3>7.2 Dbtc</h3>
<p>At each admission point, after the tagged id is resolved to a record: if the record is
an alias, look up its rules against the request’s table; the resulting <code>USER</code>
replaces the alias as the sticky record and in what Dbtc forwards to Dblqh and Dbspj.
Dblqh and Dbspj are unchanged. Plain <code>USER</code>s pay nothing new. Query threads
that lack the record locally already fall back to the table’s database record
(<code>DblqhMain.cpp:9196</code>); the same fallback applies.</p>

<h3>7.3 Management commands</h3>
<pre><code>ndb_mgm&gt; USER ALIAS SET  Marketing_mary01ab    --bucket=clicks_own        --table=marketing.marketing_clicks_1
ndb_mgm&gt; USER ALIAS SET  Recommender2_rosa9e1f --bucket=rec2_on_marketing --database=marketing
ndb_mgm&gt; USER ALIAS SET  Recommender_alice4f2  --bucket=recommender
ndb_mgm&gt; USER ALIAS DROP Marketing_mary01ab [--table=… | --database=…]
ndb_mgm&gt; USER ALIAS GET  Marketing_mary01ab
ndb_mgm&gt; USER ALIAS LIST</code></pre>
<p>Each <code>SET</code> adds or replaces one rule, creating the alias if needed;
<code>DROP</code> without a scope drops the whole alias. The management server side
mirrors the existing <code>set user</code> family in <code>Services.cpp</code>.</p>

<h3>7.4 What the other components do</h3>
<div class="tablewrap"><table>
  <thead><tr><th scope="col">Component</th><th scope="col">Change</th></tr></thead>
  <tbody>
    <tr><td>MySQL server</td><td>None. <code>ndb_user_rate_limits</code> must be on; it is read-only at runtime, so it is set in the server configuration by rondb-helm.</td></tr>
    <tr><td>Data nodes</td><td><code>ActivateRateLimits=true</code> in the cluster configuration.</td></tr>
    <tr><td>RDRS</td><td>Tag REST transactions with the project-user string (username mode, <code>RONDB-978-Cont</code>). The project is the one that owns the target database, as that branch already does for shared stores.</td></tr>
    <tr><td>ClusterJ / onlinefs</td><td>Optional: call <code>setUserId</code> with the service’s account name. Not in a first version.</td></tr>
    <tr><td>NDB API</td><td>None.</td></tr>
  </tbody>
</table></div>

<h3>7.5 To verify before committing</h3>
<ul>
  <li>Dictionary object limits: aliases scale as members × projects per member × data
  rows. Six in the example; thousands in a large deployment.</li>
  <li>Restart and backup paths for a new object flag.</li>
  <li>The cost of the rule lookup on the <code>TCKEYREQ</code> path, and sign-off on the
  new management syntax.</li>
  <li>The 60-second client re-list: a recreated alias is invisible to a connected mysqld
  for up to a minute.</li>
</ul>
</section>

"""

# ---------------- §9 sync extension
s7=must(s7,"""  <ol>
    <li>Reads the three <code>rate_limit_bucket</code> rows.</li>
    <li>Converts each percentage using the data nodes’ thread count. With 20 threads:
    20% → 4,000,000, 2% → 400,000, 5% → 1,000,000 (<a href="#mechanics">section 13</a>).</li>
    <li>Runs <code>USER LIST</code> and fixes the differences:
    <pre><code>USER SET recommender       --rate-per-sec = 4000000
USER SET clicks_own        --rate-per-sec = 400000
USER SET rec2_on_marketing --rate-per-sec = 1000000</code></pre>
    A changed budget becomes <code>USER ALTER</code>; a bucket that is gone from Hopsworks
    becomes <code>USER DROP</code>.</li>
  </ol>""","""  <ol>
    <li>Reads the three <code>rate_limit_bucket</code> rows and the rows they point at.</li>
    <li>Expands <strong>Who</strong> into account names: a uid row becomes every account
    that user holds, clip31(<code>projectname</code> + “_” + <code>username</code>) for
    each of their <code>project_team</code> rows; a project row becomes the same for every
    member (<a href="#sqlpath">section 6.1</a>). Expands <strong>What</strong> into
    scopes: a feature group row becomes <code>--table=db.name_version</code>, a store row
    <code>--database=db</code>, scope <code>ALL</code> no option.</li>
    <li>Keeps only what this cluster can enforce: buckets with at least one scope whose
    database exists on its own data connection, plus all <code>ALL</code> buckets
    (<a href="#multi">section 11</a>).</li>
    <li>Converts each percentage using the data nodes’ thread count, read from the cluster
    configuration. With 20 threads:
    20% → 4,000,000, 2% → 400,000, 5% → 1,000,000 (<a href="#mechanics">section 13</a>).</li>
    <li>Runs <code>USER LIST</code> and <code>USER ALIAS LIST</code> and fixes the
    differences, in bucket-id order:
    <pre><code>USER SET recommender       --rate-per-sec = 4000000
USER SET clicks_own        --rate-per-sec = 400000
USER SET rec2_on_marketing --rate-per-sec = 1000000
USER ALIAS SET Recommender_alice4f2  --bucket=recommender
USER ALIAS SET Recommender_ava3b7c0  --bucket=recommender
USER ALIAS SET Marketing_mary01ab    --bucket=clicks_own        --table=marketing.marketing_clicks_1
USER ALIAS SET Marketing_mike7c3d    --bucket=clicks_own        --table=marketing.marketing_clicks_1
USER ALIAS SET Recommender2_rosa9e1f --bucket=rec2_on_marketing --database=marketing</code></pre>
    A changed budget becomes <code>USER ALTER</code>; a bucket that is gone from Hopsworks
    becomes <code>USER DROP</code> plus <code>USER ALIAS DROP</code> for its accounts; a
    member who left loses their alias rule. (The service accounts
    <code>Recommender_onlinefs</code> and <code>Recommender_srvmanager</code> are included
    or excluded per <a href="#open">section 14</a>.)</li>
  </ol>""")
s7=must(s7,"""The loop also repairs entries lost to a cluster restart or a manual drop.</p>""",
"""The loop also repairs entries lost to a cluster restart or a manual drop. Two more
rules: the loop <strong>never drops on a failed read</strong> (an unreachable metadata
connection skips the run; an empty result must not be mistaken for “no buckets”), and
it <strong>reports a state per bucket per cluster</strong>: pending, installed, refused
(the 50% cap is full, <code>CreateDbNoAvailableRates</code>), or inert
(<code>SELECTED</code> with no rows). The admin page reads that state back.</p>
<p>A membership change is picked up the same way: the loop re-expands every run, so a
new member’s account gets its alias rule on the next run, and until then that member’s
SQL traffic runs unmetered. Under the section-5 maps the same change takes effect at the
next RDRS cache refresh instead.</p>""")
s7=must(s7,"""next sync plus cache refresh, Alice’s reads are tagged with a name the cluster does not
  know yet, and run unlimited.""","""next sync plus cache refresh, Alice’s reads are tagged with a name the cluster does not
  know yet (or with an alias that has no rule yet), and run unlimited.""")

# ---------------- §11 multi additions
s9=must(s9,"""and is not needed for a single cluster.</p>
</section>""","""and is not needed for a single cluster.</p>
<p>Working the design through this shape raised five points, recorded here so they are
not lost:</p>
<ul>
  <li>An <code>ALL</code> bucket means “N% of <em>each</em> cluster”. Every cluster
  converts the percentage with its own thread count, which the sync reads from its own
  cluster configuration.</li>
  <li>The 50% cap is per cluster. The sync installs in bucket-id order so that a refusal
  is deterministic, and the refused state is shown per cluster on the admin page.</li>
  <li>Never <code>USER DROP</code> because a metadata read failed; a satellite that loses
  its connection to the main cluster must keep its limits.</li>
  <li>Alice’s request for Marketing’s data arrives at M’s RDRS or M’s MySQL server, and
  both read the same bucket tables from the main cluster. Routing a request to the right
  cluster, and feature views that span clusters, are pre-existing platform questions, not
  bucket questions.</li>
  <li>The override table needs <code>ClusterName</code> in the RDRS configuration,
  templated by rondb-helm (default: the release namespace) and set by the deployment
  tooling, which also registers the name in Hopsworks for the page’s dropdown. Today
  neither RDRS, helm nor Hopsworks knows a cluster’s name.</li>
</ul>
</section>""")

# ---------------- §12 build table
s10=must(s10,"""    <tr><td>RDRS cache</td><td>Load the three tables with the existing Hopsworks metadata; build the two maps in section 5.</td></tr>
    <tr><td>RDRS matching</td><td>Steps 5 to 7 of section 5. Specify the tie and multi-group rules.</td></tr>
    <tr><td>RDRS sync</td><td>The loop in section 9, including what to report when RonDB refuses a <code>USER SET</code>.</td></tr>
    <tr><td>RonDB</td><td>Nothing. Named limits, transaction tagging, and the <code>USER</code> commands are on 26.02.</td></tr>
    <tr><td>SQL traffic</td><td>Not in a first version. MySQL tags the per-project account name, not a person. <code>ALL</code> buckets could cover SQL with an account → person mapping; data-scoped SQL limits need more.</td></tr>""",
"""    <tr><td>RDRS cache</td><td>Load the three tables with the existing Hopsworks metadata. The two maps of section 5 only if the REST-only first step is taken.</td></tr>
    <tr><td>RDRS tagging</td><td>Merge username mode from <code>RONDB-978-Cont</code> so REST transactions carry the project-user name; or, for the REST-only step, steps 5 to 7 of section 5.</td></tr>
    <tr><td>RDRS sync</td><td>The loop in section 9: expansion into accounts and scopes, the local-data filter, thread count from the cluster configuration, <code>USER</code> and <code>USER ALIAS</code> reconciliation, state reporting.</td></tr>
    <tr><td>RonDB</td><td>Section 7: the <code>USER ALIAS</code> object in Dbdict (not budgeted, persisted, backed up); the rule lookup in Dbtc admission; <code>USER ALIAS SET/DROP/GET/LIST</code>; MTR tests including a <code>JOIN</code> across two aliased tables and a SQL session under a table-scoped alias. Named limits, tagging and the <code>USER</code> commands are already on 26.02.</td></tr>
    <tr><td>MySQL server</td><td>Nothing. Enable <code>ndb_user_rate_limits</code> through rondb-helm; <code>ActivateRateLimits</code> on the data nodes.</td></tr>
    <tr><td>SQL traffic</td><td>Covered by sections 6 and 7 once the alias exists. Until then, only REST traffic is metered by buckets.</td></tr>""")

# ---------------- §13 reference: cap rationale
s11=must(s11,"""Buckets do not raise the cap and do not reserve CPU for their members.</p>""",
"""Buckets do not raise the cap and do not reserve CPU for their members.</p>
<p><strong>Why the cap exists.</strong> The meter only charges row operations: the reads,
writes and scanned rows in the table above. Redo logging, local and global checkpoints,
send and receive threads, schema operations, backups, and every unmetered identity (the
admin account, unknown names, databases without a quota) also need CPU and are never
charged. RonDB sizes the pool as all block and send threads × 1,000,000 and lets
configured rates sum to half of it (70% when altering an existing entry; a restart never
refuses) (<code>Dbdict::initCommonData</code>). Half the node is kept for the work the
meter cannot see, so that even with every bucket running at its ceiling the metered work
cannot saturate the node, and the ceilings keep meaning something. A rate of 0 means
unlimited and consumes nothing from the pool. For this design the cap is why there is one
budgeted <code>USER</code> per bucket and why aliases (section 7) are deliberately free.</p>""")

# ---------------- §14 open decisions
s_open = """<section id="open">
<h2>14. Open decisions</h2>
<ol>
  <li><strong><code>data_scope</code> column</strong> (section 8): keep it, so a cascade
  from a dropped feature group cannot widen a bucket to <code>ALL</code>.</li>
  <li><strong>Service accounts in project rows</strong> (section 3.2): include
  <code>onlinefs</code> and <code>srvmanager</code>, or exclude them by role.</li>
  <li><strong>A tie created by a membership change</strong> (section 8): refuse the
  change, or accept it and mark both buckets conflicting.</li>
  <li><strong>REST-only first?</strong> (section 6.6): ship the section-5 maps in RDRS
  before the alias lands in RonDB, or go straight to the alias.</li>
  <li><strong>The superuser execution path</strong> in RDRS for partially shared feature
  groups and restricted roles: attribute to the caller, or exempt explicitly.</li>
  <li><strong>Transactions over several tables</strong> (feature views): accept
  first-table-wins as the kernel does, or have RDRS pre-pick the most restrictive bucket
  and tag the bucket name directly.</li>
  <li><strong>Several clusters</strong> (section 11): whether <code>ClusterName</code> and
  the override table are in scope now.</li>
  <li><strong>Tickets</strong>: Hopsworks (tables, page), RonDB kernel (alias), RDRS
  (sync, tagging), deployment (settings). Three unrelated Hopsworks defects found while
  scouting deserve their own tickets: the inverted <code>RONDB_QUOTAS_ENABLED</code> check
  in <code>ProjectQuotasController</code> (lines 309 and 320), cross-satellite share
  grants that silently do nothing, and <code>DatasetSharedWithFacade.java:169</code>
  filtering on the dropped <code>accepted</code> column.</li>
</ol>
</section>

"""

# ---------------- §15 appendix: title + mgm syntax
s12=must(s12,"<h2>15. Appendix: proposed table definitions</h2>","<h2>15. Appendix: proposed table definitions and commands</h2>")
s12=must(s12,"""code, as the existing sharing tables do; the unique keys prevent duplicate rows.</p>
</section>""","""code, as the existing sharing tables do; the unique keys prevent duplicate rows.</p>
<h3>Proposed management commands (section 7)</h3>
<pre><code>USER ALIAS SET  &lt;name&gt; --bucket=&lt;user&gt; [--table=&lt;db&gt;.&lt;table&gt; | --database=&lt;db&gt;]
USER ALIAS DROP &lt;name&gt; [--table=&lt;db&gt;.&lt;table&gt; | --database=&lt;db&gt;]
USER ALIAS GET  &lt;name&gt;
USER ALIAS LIST</code></pre>
<p>The management-server protocol commands mirror the existing <code>set user</code> /
<code>alter user</code> / <code>drop user</code> / <code>get user</code> /
<code>list user</code> family (<code>Services.cpp</code>), with the arguments
<code>username</code>, <code>bucket</code>, and an optional <code>database</code> or
<code>table</code>.</p>
</section>""")

footer=old[old.index('<footer>'):]
footer=must(footer,"""rondb-helm, and deploy.hopsworks.ai source reviews recorded with this design.""",
"""rondb-helm, and deploy.hopsworks.ai source reviews recorded with this design; the
  MySQL-server tagging path (<code>ha_ndbcluster.cc</code>, <code>ClusterMgr.cpp</code>,
  <code>DbtcMain.cpp</code>, <code>DblqhMain.cpp</code>) and the Hopsworks online-account
  code (<code>OnlineFeaturestoreController</code>, <code>OnlineFeaturestoreFacade</code>)
  reviewed 2026-10-21.""")

design = head.replace('<title>RonDB rate-limit buckets: the Hopsworks tables</title>','<title>RonDB rate-limit buckets: design</title>') \
  + '<body>\n<main>\n' + header + '\n\n' + nav + '\n' + s1+'\n'+s2+'\n'+s3+'\n'+s4+'\n'+s5+'\n'+s_sql+s_kernel+s6+'\n'+s7+'\n'+s8+'\n'+s9+'\n'+s10+'\n'+s11+'\n'+s_open+s12+'\n'+footer
open(D+'RonDB-Rate-Limit-Buckets-Design.html','w',encoding='utf-8').write(design)

# ---------------------------------------------------------------- OVERVIEW DOC
def inner(block):  # strip <section ...> and </section>
    return re.sub(r'^<section id="[^"]+">\n|</section>\n$','',block)

o1=inner(sec('problem'))
o1=must(o1,"""<li>Give each key 7%? Alice’s training job hits its 7% while the other two keys sit idle.</li>
  </ul>""","""<li>Give each key 7%? Alice’s training job hits its 7% while the other two keys sit idle.</li>
    <li>And Ava’s Spark job reads the same tables through the MySQL server, where there is
    no API key at all.</li>
  </ul>""")
o1=must(o1,"""When Alice reads a table, RDRS finds the bucket that contains both Alice and that table,
and tags the transaction with the bucket’s name.""","""When Alice reads a table, the bucket that contains both Alice and that table is found,
and her work is charged to it, whether she came through the REST server or through the
MySQL server.""")
o1=must(o1,"<p>What the administrator wants to say is: “the Recommender team, together, gets 20%.”</p>",
"<p>What the administrator wants to say is: “the Recommender team, together, gets 20%, however they connect.”</p>")

o2=inner(sec('teams'))
o2=must(o2,'<h2>2. The teams, and what Hopsworks already stores about them</h2>','<h2>2. The teams in the examples</h2>')

ex=inner(sec('example'))
wishes=ex[ex.index('<p>The administrator has three requirements:</p>'):ex.index('<h3>Who gets charged for what</h3>')]
wishes=wishes.replace('<a href="#mechanics">section 11</a>','<a href="#budgets">section 7</a>')
charged=ex[ex.index('<h3>Who gets charged for what</h3>'):]
charged=must(charged,'<h3>Who gets charged for what</h3>','<h3>Who gets charged for what, in the example</h3>')
charged=re.sub(r'\s*Her reads of <code>marketing_campaigns_1</code> still go to\s*<code>rec2_on_marketing</code>\. The two budgets are separate: the 5% is not a cap on\s*her total\.',
 ' Her reads of <code>marketing_campaigns_1</code> still go to <code>rec2_on_marketing</code>. The two budgets are separate: the 5% is not a cap on her total.', charged)
charged=charged.replace('Mary’s read is handled the ordinary per-key way.','Mary’s read runs as today, outside any bucket.').replace('Ordinary per-key behavior.','Runs as today, outside any bucket.')

o3="""<section id="cases">
<h2>3. What you can say with a bucket</h2>
<p>A bucket has four parts. Everything in this section is a different way of filling
them in.</p>
<figure class="erd"><img src="img/bucket-concept.png" alt="Two buckets: recommender, 20% of the cluster, shared by project Recommender over all data they may read; and clicks_own, 2%, shared by Mary and Mike over the feature group marketing_clicks."><figcaption>Two buckets. The first covers everything its members read; the second covers one feature group for two named people.</figcaption></figure>
<div class="tablewrap"><table>
  <thead><tr><th scope="col">Part</th><th scope="col">What you enter</th><th scope="col">Example</th></tr></thead>
  <tbody>
    <tr><td>Name</td><td>A short name. It becomes the identity RonDB meters.</td><td><code>recommender</code></td></tr>
    <tr><td>Budget</td><td>A percentage of the cluster’s database-work capacity.</td><td>20%</td></tr>
    <tr><td>Who</td><td>Login users and/or whole projects. A project means its current members, so people who join later are covered automatically.</td><td>project Recommender</td></tr>
    <tr><td>What</td><td>Either <em>all accessible data</em>, or a list of feature groups and feature stores.</td><td>all accessible data</td></tr>
  </tbody>
</table></div>
"""+wishes+"""<p>The three wishes are the three most common things an administrator wants to say:</p>

<h3>3.1 “This team gets a share of the cluster”</h3>
<p>A team’s serving, training and exploration should together stay under a share, whatever
data they touch, including data other projects shared with them.</p>
<div class="example"><p>Bucket <code>recommender</code>, 20%. Who: project Recommender.
What: all accessible data. Alice reading her own features, Ava reading Sales’ orders and
Alice’s Spark job joining Marketing’s clicks all draw from the same 20%.</p></div>

<h3>3.2 “Protect a hot feature group from our own jobs”</h3>
<p>A feature group that serves production traffic should not be starved by the team’s
own backfills. No sharing is involved; the owner caps itself on one table.</p>
<div class="example"><p>Bucket <code>clicks_own</code>, 2%. Who: Mary, Mike. What: feature
group <code>marketing_clicks</code>. Mary’s backfill of the clicks table is held to 2%;
her reads of <code>marketing_campaigns</code> are not affected.</p></div>

<h3>3.3 “Share the data, but cap the consumer”</h3>
<p>The owner of a feature store is happy to share it, but wants a say in how much of the
cluster the consumer may burn when reading it.</p>
<div class="example"><p>Bucket <code>rec2_on_marketing</code>, 5%. Who: project
Recommender2. What: feature store <code>marketing</code>. Rosa’s reads of anything in
Marketing’s store are held to 5%. Her reads of her own data are not in this bucket.</p></div>

<h3>3.4 “Serving, batch and free tier”</h3>
<p>Buckets are not tied to the org chart. A bucket whose Who list is the service accounts
that run model serving, another for the users who run batch jobs, and a small one for a
free-tier project are the same mechanism with different lists.</p>

"""+charged+"""</section>
"""

o4="""<section id="paths">
<h2>4. Every access path counts against the same bucket</h2>
<p>Teams reach the online feature store in two ways, and a budget is only useful if both
count. The REST server identifies a caller by the owner of the API key. The MySQL server
identifies a caller by the per-member database account that Hopsworks creates in each
project. Both resolve to the same person, so both land in the same bucket.</p>
<figure class="erd"><img src="img/coverage.png" alt="Superset, Jupyter, Spark or Flink jobs and serving pods go through the MySQL server; the Python REST client and serving pods go through the RonDB REST server; both servers charge the same bucket on the data nodes."><figcaption>Different clients, two front doors, one bucket.</figcaption></figure>
<div class="tablewrap"><table>
  <thead><tr><th scope="col">How the team connects</th><th scope="col">Identified by</th><th scope="col">Counted</th></tr></thead>
  <tbody>
    <tr><td>Python or Java feature-store client, REST reads</td><td>API key → its owner</td><td>yes</td></tr>
    <tr><td>Spark or Flink jobs, Jupyter, Superset, SQL from any tool</td><td>the member’s database account</td><td>yes</td></tr>
    <tr><td>Model-serving deployments</td><td>the API key or account of the person who started the deployment</td><td>yes</td></tr>
    <tr><td>Online ingestion by the platform</td><td>the platform’s own identity</td><td>only if an administrator puts that identity in a bucket</td></tr>
    <tr><td>Hopsworks’ own administrative queries (schema changes, grants, metadata)</td><td>the admin account</td><td>no, by design</td></tr>
  </tbody>
</table></div>
<p>Behind the scenes, finding the bucket is two lookups: which buckets list this person,
and which buckets list this data, most specific first. The picture below shows the REST
server doing it for Alice; the MySQL path ends in the same place.</p>
<figure class="erd"><img src="img/lookup-maps.png" alt="The two lookup maps built from the bucket rows, and Alice’s request passing through them to bucket recommender."><figcaption>The two maps built from the bucket rows, and Alice’s request passing through them. Grey italic text is explanation, not a column.</figcaption></figure>
<p>When two buckets both match, the more specific one wins: a bucket on a feature group
beats one on its store, which beats one on all data. Hopsworks refuses to save two
buckets that would tie.</p>
</section>
"""

o5=inner(sec('admin'))
o5=must(o5,'<h2>8. The admin page</h2>','<h2>5. The admin page</h2>')
o5=must(o5,"""right. Each form field writes to exactly one table, shown in the right margin. The list
also shows the cluster state from <a href="#sync">section 7</a>, so an administrator can
see when a saved bucket has actually been installed.</p>""","""right. The list also shows whether each bucket is live on the cluster, so an administrator
can see when a saved bucket has actually been installed. (The right margin of the mockup,
naming the table each field writes to, is for the design document and would not appear
in the product.)</p>""")
o5=must(o5,"""What were filled in as intended.</p>""","""What were filled in as intended.</p>
<div class="tablewrap"><table>
  <thead><tr><th scope="col">Cluster column</th><th scope="col">Meaning</th></tr></thead>
  <tbody>
    <tr><td>installed</td><td>The limit is live on the cluster.</td></tr>
    <tr><td>pending</td><td>Saved in Hopsworks; the cluster does not have it yet. Until it does, that traffic runs unlimited.</td></tr>
    <tr><td>refused</td><td>The cluster rejected it because the total budget is already full (<a href="#budgets">section 7</a>). Lower another budget first.</td></tr>
    <tr><td>inert</td><td>A selected-data bucket whose data rows are gone (its feature group was deleted). It matches nothing.</td></tr>
  </tbody>
</table></div>
<p>Who may create buckets is a product decision. A natural split is that administrators
create all-data buckets, while the data owner of a feature store or feature group may
cap its own data or its consumers.</p>""")

mech=inner(sec('mechanics'))
caller=mech[mech.index('<h3>What a limited caller sees</h3>'):mech.index('<h3>Managing names</h3>')]
pct=mech[mech.index('<h3>Turning a percentage into a rate</h3>'):mech.index('<h3>What a limited caller sees</h3>')]
pct=must(pct,"""Buckets do not raise the cap and do not reserve CPU for their members.</p>""",
"""Buckets do not raise the cap and do not reserve CPU for their members.</p>
<p><strong>Why the cap exists.</strong> The meter only counts row operations: the reads,
writes and scans a user causes. Redo logging, checkpoints, network threads, schema
changes, backups and the platform’s own unmetered work also need CPU, and are never
charged to anyone. Half the machine is kept for them, so that even when every bucket runs
at its ceiling the node is not saturated and the limits still mean something.</p>
<div class="tablewrap"><table>
  <thead><tr><th scope="col">Budget</th><th scope="col">On 20-thread data nodes</th><th scope="col">Roughly</th></tr></thead>
  <tbody>
    <tr><td>20%</td><td>4,000,000 per second</td><td>about 3 million small key reads per second per node</td></tr>
    <tr><td>5%</td><td>1,000,000 per second</td><td>about 800,000 small key reads per second per node</td></tr>
    <tr><td>2%</td><td>400,000 per second</td><td>about 320,000 small key reads per second per node</td></tr>
  </tbody>
</table></div>
<p>If a new bucket would push the total over 50%, the cluster refuses it and the admin
page shows <em>refused</em>.</p>""")

o6="""<section id="experience">
<h2>6. What a limited team experiences</h2>
"""+caller.replace('<h3>What a limited caller sees</h3>\n','')+"""<p>In order, under sustained excess:</p>
<ol>
  <li><strong>Slower.</strong> New transactions from the bucket are queued for a short
  time before they start. Latency rises; nothing fails.</li>
  <li><strong>Reads rejected.</strong> If the excess continues, reads get a temporary
  error: HTTP 429 from the REST server, MySQL error 1297 with the RonDB reason from the
  MySQL server. The Hopsworks clients already retry 429.</li>
  <li><strong>Writes rejected</strong> only under heavy, sustained overload.</li>
</ol>
<p>Rejections carry no risk to data, and nobody outside the bucket is affected, which is
the point.</p>
</section>
"""

o7="""<section id="budgets">
<h2>7. How budgets add up</h2>
<p>A budget is entered as a percentage and converted into RonDB’s unit, CPU microseconds
of accounted work per second on each data node, using the data nodes’ thread count.</p>
"""+pct+"""</section>
"""

o8="""<section id="multi">
<h2>8. Several clusters</h2>
<p>Some deployments give each big project its own RonDB cluster (M for Marketing, S for
Sales, R for Recommender). The owner of each cluster then wants to say “Recommender may
use 20% of <em>my</em> cluster, Recommender2 30%.” The same buckets express that, and
nothing names a cluster: a bucket on Marketing’s data can only ever be enforced where
Marketing’s data lives.</p>
<figure class="erd"><img src="img/multi-cluster-topology.png" alt="Hopsworks holds one set of bucket tables; cluster M installs the two Marketing-data buckets, cluster S the Sales one, cluster R none."><figcaption>One set of buckets in Hopsworks. Each cluster installs only the buckets whose data it holds. A bucket on all accessible data means that percentage on every cluster.</figcaption></figure>
<p>The rows behind that picture are the same three tables as before, and no row names a
cluster:</p>
<figure class="erd"><img src="img/multi-cluster.png" alt="The same three tables with a cluster per project. No row names a cluster; each cluster’s RDRS installs the buckets whose data it holds."><figcaption>The same three tables with a cluster per project. No row names a cluster; each cluster installs the buckets whose data it holds.</figcaption></figure>
<p>Only one thing would ever need a cluster name: giving an all-data bucket a different
size on one cluster (“Recommender gets 20% everywhere but 40% on R”). That is a small
addition described in the design document and is not needed for a single cluster.</p>
</section>
"""

o9="""<section id="rollout">
<h2>9. What you need, and how to roll it out</h2>
<div class="tablewrap"><table>
  <thead><tr><th scope="col">Component</th><th scope="col">Requirement</th></tr></thead>
  <tbody>
    <tr><td>RonDB</td><td>26.02 or later with rate limits activated in the cluster configuration, plus the bucket support described in the design document.</td></tr>
    <tr><td>MySQL servers</td><td>The RonDB user-rate-limit setting turned on, so SQL sessions are tagged with the member account. A server setting managed by the deployment, not by users.</td></tr>
    <tr><td>Hopsworks</td><td>A version that ships the bucket tables and the admin page.</td></tr>
    <tr><td>Existing data and projects</td><td>Nothing changes. Projects not in any bucket behave exactly as today.</td></tr>
  </tbody>
</table></div>
<p>A sensible first week:</p>
<ol>
  <li>Create one all-data bucket for the team that has caused trouble before, at a
  generous budget. Watch the cluster column turn to <em>installed</em>.</li>
  <li>Observe. 429s or 1297s in that team’s logs tell you the ceiling was hit.</li>
  <li>Tighten, and add buckets for shared feature stores whose consumers you want to cap.</li>
</ol>
</section>

<section id="faq">
<h2>10. Questions we expect</h2>
<dl>
  <dt><strong>Does a bucket restrict who can read what?</strong></dt>
  <dd>No. Access is decided by project membership and sharing exactly as today. A bucket
  only decides which budget pays for work that is already allowed.</dd>
  <dt><strong>Someone joins or leaves the team. Do I edit the bucket?</strong></dt>
  <dd>Not if the bucket names the project; membership is read live. If the bucket names
  people individually, you edit the list.</dd>
  <dt><strong>We created version 2 of a feature group that is in a bucket.</strong></dt>
  <dd>A feature-group row in a bucket is one version. Add version 2, or put the feature
  store in the bucket instead; that covers every feature group and version in it,
  including future ones.</dd>
  <dt><strong>Can a person be in two buckets?</strong></dt>
  <dd>Yes, as long as they do not tie. Mary can be capped on one feature group and belong
  to a team-wide bucket; the feature-group bucket wins for that table. Two all-data
  buckets that both contain Mary are refused.</dd>
  <dt><strong>A query touches several feature groups, such as a feature view.</strong></dt>
  <dd>RonDB charges a transaction to one bucket, the first one it touches. The design
  document discusses choosing it deliberately.</dd>
  <dt><strong>Is 20% a guarantee?</strong></dt>
  <dd>No. It is the most the team may use. If the cluster is idle the team is still held
  to 20%; if the cluster is busy the team is still allowed up to 20%.</dd>
  <dt><strong>We have several clusters. Does a bucket cover all of them?</strong></dt>
  <dd>An all-data bucket means that percentage on each cluster. A bucket on specific data
  is enforced on the cluster that holds the data.</dd>
</dl>
</section>
"""

ohead="""<header>
  <p class="eyebrow">RonDB · Hopsworks · Feature overview · October 2026</p>
  <h1>Rate-limit buckets: a shared database budget for every team</h1>
  <p class="lead">Today RonDB can limit how much database work one API key may cause.
  That is the wrong unit. A team has several people, each person has several keys, and
  half the team’s work arrives through the MySQL server with no key at all. What an
  administrator actually wants to say is “this team may use 20% of the cluster” or “this
  team may use 20% of <em>our</em> cluster when it reads <em>our</em> data”, and have it
  hold however the team connects.</p>
  <p>The proposal adds one new concept, the <strong>rate-limit bucket</strong>: a named
  budget that a group of people share, applied either to everything they read or to
  specific feature groups and feature stores. An administrator creates buckets on one
  page in Hopsworks; RonDB enforces them using the rate-limit feature it already has.</p>
  <p>This document explains what a bucket is, the situations it is meant for, what the
  administrator sees, and what a team that is over budget experiences. A companion
  document, <em>Rate-limit buckets: design</em>, covers the tables, the servers and the
  RonDB changes behind it.</p>
  <p class="meta">Status: proposed feature. RonDB’s per-name rate limiting exists today
  (RonDB 26.02). Buckets, the Hopsworks page and the automatic installation are new work.</p>
</header>

<nav aria-label="Contents">
  <ol>
    <li><a href="#problem">The problem and the idea</a></li>
    <li><a href="#teams">The teams in the examples</a></li>
    <li><a href="#cases">What you can say with a bucket</a></li>
    <li><a href="#paths">Every access path counts against the same bucket</a></li>
    <li><a href="#admin">The admin page</a></li>
    <li><a href="#experience">What a limited team experiences</a></li>
    <li><a href="#budgets">How budgets add up</a></li>
    <li><a href="#multi">Several clusters</a></li>
    <li><a href="#rollout">What you need, and how to roll it out</a></li>
    <li><a href="#faq">Questions we expect</a></li>
  </ol>
</nav>
"""
ofoot="""<footer>
  <p class="meta">Companion document: <em>Rate-limit buckets: design</em>
  (<code>RonDB-Rate-Limit-Buckets-Design.html</code>). Examples describe proposed
  behavior; they are not a claim that the feature has shipped.</p>
</footer>
</main>
</body>
</html>
"""
overview = head.replace('<title>RonDB rate-limit buckets: the Hopsworks tables</title>','<title>RonDB rate-limit buckets: overview</title>') \
  + '<body>\n<main>\n' + ohead + '\n<section id="problem">\n'+o1+'</section>\n\n<section id="teams">\n'+o2+'</section>\n\n'+o3+'\n'+o4+'\n<section id="admin">\n'+o5+'</section>\n\n'+o6+'\n'+o7+'\n'+o8+'\n'+o9+'\n'+ofoot
# overview is now hand-written (feature-only, admin UI image only); not generated
# open(D+'RonDB-Rate-Limit-Buckets-Overview.html','w',encoding='utf-8').write(overview)
print('ok', len(design), len(overview))
