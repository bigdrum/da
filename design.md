# Dabase

**March 2017**

## OVERVIEW

Dabase or Da is provides a couchdb like document store solution based on SQL Database (currently is PostgresSQL, in theory, it should work on CockroachDB which provides horizontal scaling).

It is implemented purely on client side library so no extra server deployment is required.

## GOALS

1. Implements all features of couchdb. Notably, mapreduce view, change feed, versioned, master-master replication.

2. Explore more advanced feature, such as multi-key transaction. Multi-stage mapreduce etc.

3. Explore full text search.

## SPECIFICATIONS

### External Objects

Database - A database maps to a SQL database. It is equivalent to a installation of CouchDB server.

Table - A table is equivalent to a CouchDB database, and internally data of the same table is stored in the same PostgresSQL table.

Document - A document is equivalent to a couchdb document. A document is identified by an ID. Data stored in the document is versioned. All revision history is kept by Da.

### Data Table schemas

For each Da Table, there is a data table created on PostgresSQL. Each schema would look like:

<table>
  <tr>
    <td>Column Name</td>
    <td>Description</td>
  </tr>
  <tr>
    <td>seq</td>
    <td>Primary key, auto incremented</td>
  </tr>
  <tr>
    <td>id</td>
    <td>The document id, text.</td>
  </tr>
  <tr>
    <td>version</td>
    <td>The version, started from zero, incremented each time the document is modified.</td>
  </tr>
  <tr>
    <td>uuid</td>
    <td>A globally unique id to help replication.</td>
  </tr>
  <tr>
    <td>data</td>
    <td>A json of actual data.</td>
  </tr>
  <tr>
    <td>timestamp</td>
    <td>When the given version is created.</td>
  </tr>
  <tr>
    <td>latest</td>
    <td>A boolean to indicate this version is the latest one.</td>
  </tr>
  <tr>
    <td>deleted</td>
    <td>A boolean to indicate this version is a deletion of the document.</td>
  </tr>
</table>


### Metadata

Da needs to store some extra metadata. So it would create SQL tables for

### Multiple concurrent clients support

Even though Da is implemented as a client side library, when the library is instantiated multiple times to connect to a single Database, they should work together with some consensus. Some of the feature is naturally supported, such as simple read/write. Some might not, we will discuss the design of such situation per feature.

### View

In CouchDB, the user could store a design doc which contains the map and reduce logic, that will be used to generate the view. For Da, we could provide even more flexibility. For example, the user could pass the native logic of mapreduce directly to the Da library.

The result of view is stored in the view’s data table. The result is created lazily. When a table is modified, its seq number is incremented. This would trigger the view process the new data and update its result. The view stores the latest seq number that it has processed. View update can be done lazily. When the client reads the result of the view, it could require the view to be up-to-date or stale.

The view process should be incremental. This can be done naturally with CouchDB like map-reduce.

To support multi-client, the naive solution would be for every client to just create a transaction to update the view. If more than one client attempts to update the same view, it will cause transaction conflict, so only one client would win. Since the view generation should be idempotent, this will guarantee the view result is correct, but it might cause wasteful duplicate work.

Another issue with multi-client is that each client could run different version of the view logic. So the view logic needs to be versioned, and the view result is associated with a version.

**View result schema, one table per view.**

<table>
  <tr>
    <td>Column name</td>
    <td>Description</td>
  </tr>
  <tr>
    <td>seq</td>
    <td>Primary key, auto_increment.</td>
  </tr>
  <tr>
    <td>view_version</td>
    <td>Version of the view.</td>
  </tr>
  <tr>
    <td>key</td>
    <td>Key output by the view.</td>
  </tr>
  <tr>
    <td>value</td>
    <td>Value output by the view.</td>
  </tr>
  <tr>
    <td>doc_id</td>
    <td>ID of input document, for map result only.</td>
  </tr>
</table>


**View metadata**

<table>
  <tr>
    <td>Column</td>
    <td>Description</td>
  </tr>
  <tr>
    <td>view_name</td>
    <td></td>
  </tr>
  <tr>
    <td>view_version</td>
    <td></td>
  </tr>
  <tr>
    <td>table_name</td>
    <td></td>
  </tr>
  <tr>
    <td>table_seq</td>
    <td></td>
  </tr>
</table>


### Streaming change feed

When a change feed is created, the Da would setup a trigger on the data table and use PostgresSQL Notify/Listen for the change. Initially, Da should also send changes happened before the Notify/Listen is setup, which involves scanning all the latest version rows ordered by the seq number.

Because the use of Notify/Listen, the change from other client is correctly handled.

### Maintenance

### Master-master replication

Da does not plan to support master-master replication. It would rely on the undelying SQL DB replication.

### Conditional query and ordering

Da would expose the SQL DB’s SELECT feature on both table and view, to make it possible to support complex query without the need to build extra view.

## MILESTONES

TBD
