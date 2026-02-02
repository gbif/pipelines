package org.gbif.pipelines.spark;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.monotonically_increasing_id;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.graphx.Edge;
import org.apache.spark.graphx.EdgeTriplet;
import org.apache.spark.graphx.Graph;
import org.apache.spark.graphx.Pregel;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;
import org.gbif.pipelines.io.avro.Parent;
import scala.Tuple2;
import scala.Tuple3;
import scala.reflect.ClassTag;
import scala.runtime.AbstractFunction2;

@Slf4j
public class CalculateLineage {

  public static void main(String[] args) {

    java.util.List<Tuple3<String, String, String>> events =
        java.util.List.of(
            new Tuple3<>("Event1", "TypeA", null),
            new Tuple3<>("Event2", "TypeB", "Event1"),
            new Tuple3<>("Event3", "TypeC", "Event1"),
            new Tuple3<>("Event4", "TypeD", "Event2"));

    SparkSession.Builder sparkBuilder = SparkSession.builder().appName("graphx test");
    sparkBuilder = sparkBuilder.master("local[*]");
    SparkSession spark = sparkBuilder.getOrCreate();
    Dataset<Row> eventDf =
        spark
            .createDataset(
                events, Encoders.tuple(Encoders.STRING(), Encoders.STRING(), Encoders.STRING()))
            .select(
                col("_1").as("eventId"), col("_2").as("eventType"), col("_3").as("parentEventId"))
            .toDF();
    Dataset<EventLineage> lineages = calculateLineage(spark, eventDf);

    spark.close();
  }

  /**
   * Calculates lineage using GraphX Pregel API
   *
   * @param spark
   * @param eventDf
   */
  public static Dataset<EventLineage> calculateLineage(SparkSession spark, Dataset<Row> eventDf) {

    Dataset<Row> eventDfWithId = eventDf.withColumn("vertexId", monotonically_increasing_id());

    eventDfWithId.createOrReplaceTempView("events");

    String sqlQuery =
        """
            SELECT
                concat(l.eventId, '||', coalesce(l.eventType,'')) AS eventId,
                concat(l.parentEventId, '||', coalesce(r.eventType,'')) AS parentEventId,
                l.vertexId AS vertexId,
                r.vertexId AS parentVertexId
            FROM events l
            LEFT OUTER JOIN events r
            ON l.parentEventId = r.eventId
        """;
    Dataset<Row> joined = spark.sql(sqlQuery);

    // create vertices RDD
    RDD<Tuple2<Object, EventNode>> verticesRdd =
        joined
            .toJavaRDD()
            .map(
                row ->
                    new Tuple2<>(
                        (Object) row.getLong(2), new EventNode(row.getString(0), row.getString(1))))
            .rdd();

    RDD<Edge<String>> edgesRdd =
        joined
            // Remove rows where supervisorId is null
            .filter("parentVertexId IS NOT NULL")
            // Select supervisorId, eventId, role
            .select("parentVertexId", "vertexId", "eventId")
            // Convert to RDD<Edge<String>>
            .javaRDD()
            .map(
                row ->
                    new Edge<>(
                        row.getLong(0), // parentEventId (source vertex)
                        row.getLong(1), // eventId (destination vertex)
                        row.getString(2) // role (edge attribute)
                        ))
            .rdd();

    EventNode missingEvent = new EventNode("EMPTY", "EMPTY");

    ClassTag<EventNode> eventNodeTag = scala.reflect.ClassTag$.MODULE$.apply(EventNode.class);
    ClassTag<EventNodeMessage> eventNodeMessageTag =
        scala.reflect.ClassTag$.MODULE$.apply(EventNodeMessage.class);
    ClassTag<EventNodeValue> eventNodeValueTag =
        scala.reflect.ClassTag$.MODULE$.apply(EventNodeValue.class);

    ClassTag<String> stringTag = scala.reflect.ClassTag$.MODULE$.apply(String.class);

    // Build the graph
    Graph<EventNode, String> eventGraph =
        Graph.apply(
            verticesRdd,
            edgesRdd,
            missingEvent,
            StorageLevel.MEMORY_ONLY(),
            StorageLevel.MEMORY_ONLY(),
            eventNodeTag,
            stringTag);

    // Map vertices
    Graph<EventNodeValue, String> eventNodeValueGraph =
        eventGraph.mapVertices(new EventNodeMapper(), eventNodeValueTag, null);

    EventNodeMessage initialMsg =
        new EventNodeMessage(
            0L,
            0,
            "",
            new ArrayList<>(), // empty list
            false,
            true);

    // <EventNodeValue, String, EventNodeMessage>
    var results =
        Pregel.<EventNodeValue, String, EventNodeMessage>apply(
            eventNodeValueGraph,
            initialMsg,
            Integer.MAX_VALUE,
            org.apache.spark.graphx.EdgeDirection.Out(),
            new VProg(),
            new SendMsg(),
            new MergeMsg(),
            eventNodeValueTag,
            stringTag,
            eventNodeMessageTag);

    StructType parentSchema =
        DataTypes.createStructType(
            new StructField[] {
              DataTypes.createStructField("id", DataTypes.StringType, true),
              DataTypes.createStructField("eventType", DataTypes.StringType, true),
              DataTypes.createStructField("verbatimEventType", DataTypes.StringType, true),
              DataTypes.createStructField("order", DataTypes.IntegerType, true)
            });

    // convert results back to DataFrame if needed
    return spark
        .createDataFrame(
            results
                .vertices()
                .toJavaRDD()
                .map(
                    v -> {
                      EventNodeValue val = v._2();
                      AtomicInteger order = new AtomicInteger();

                      scala.collection.mutable.ArrayBuffer<Row> parentRowsBuffer =
                          new scala.collection.mutable.ArrayBuffer<>();

                      List<String> path = val.getPath();
                      Collections.reverse(path);
                      path.forEach(
                          node -> {
                            if (node != null && !node.equals("EMPTY")) {
                              String[] parts = node.split("\\|\\|");
                              if (parts.length < 2) {
                                // parts = new String[] {parts[0], ""};
                              } else {
                                parentRowsBuffer.$plus$eq(
                                    RowFactory.create(
                                        parts[0], // id
                                        parts[1], // eventType
                                        parts[1], // verbatimEventType
                                        order.getAndIncrement() // order
                                        ));
                              }
                            }
                          });
                      return RowFactory.create(val.getName(), parentRowsBuffer.toSeq());
                    }),
            DataTypes.createStructType(
                Arrays.asList(
                    DataTypes.createStructField("eventId", DataTypes.StringType, false),
                    DataTypes.createStructField(
                        "lineage", DataTypes.createArrayType(parentSchema), false))))
        .map(
            (MapFunction<Row, EventLineage>)
                row -> {
                  String eventId = row.getString(0).split("\\|\\|")[0];
                  List<Parent> parents = new ArrayList<>();
                  List<Row> parentRows = row.getList(1);
                  for (Row parentRow : parentRows) {
                    Parent parent =
                        Parent.newBuilder()
                            .setId(parentRow.getString(0))
                            .setEventType(parentRow.getString(1))
                            .setVerbatimEventType(parentRow.getString(2))
                            .setOrder(parentRow.getInt(3))
                            .build();
                    parents.add(parent);
                  }
                  return new EventLineage(eventId, parents);
                },
            Encoders.bean(EventLineage.class));
  }

  // Step 1: Mutate the value of the vertices, based on the message received
  public static class VProg
      implements scala.Function3<Object, EventNodeValue, EventNodeMessage, EventNodeValue>,
          Serializable {

    @Override
    public EventNodeValue apply(Object vertexId, EventNodeValue value, EventNodeMessage message) {

      if (message.getLevel() == 0) { // superstep 0 - initialize
        return new EventNodeValue(
            value.getName(),
            value.getCurrentId(),
            value.getLevel() + 1,
            value.getName(),
            value.getPath(),
            value.isCyclic,
            value.isLeaf);
      } else if (message.isCyclic()) { // set isCyclic
        return new EventNodeValue(
            value.getName(),
            value.getCurrentId(),
            value.getLevel() + 1,
            value.getName(),
            value.getPath(),
            true,
            value.isLeaf);
      } else if (!message.isLeaf()) { // set isLeaf
        return new EventNodeValue(
            value.getName(),
            value.getCurrentId(),
            value.getLevel() + 1,
            value.getName(),
            value.getPath(),
            value.isCyclic,
            false);
      } else { // set new values
        List<String> newPath = new ArrayList<>();
        newPath.add(value.getName());
        newPath.addAll(message.getPath());
        return new EventNodeValue(
            value.getName(),
            message.getCurrentId(),
            value.getLevel() + 1,
            message.head,
            newPath,
            value.isCyclic,
            false);
      }
    }
  }

  public static class SendMsg
      implements scala.Function1<
              EdgeTriplet<EventNodeValue, String>,
              scala.collection.Iterator<Tuple2<Object, EventNodeMessage>>>,
          Serializable {

    @Override
    public scala.collection.Iterator<Tuple2<Object, EventNodeMessage>> apply(
        EdgeTriplet<EventNodeValue, String> triplet) {
      EventNodeValue src = triplet.srcAttr();
      EventNodeValue dst = triplet.dstAttr();

      scala.collection.mutable.ArrayBuffer<Tuple2<Object, EventNodeMessage>> messages =
          new scala.collection.mutable.ArrayBuffer<>();

      // Handle cyclic reporting structure
      if (src.getCurrentId() == triplet.dstId() || src.getCurrentId() == dst.getCurrentId()) {
        if (!src.isCyclic()) { // Set isCyclic
          messages.$plus$eq(
              new scala.Tuple2(
                  triplet.dstId(),
                  new EventNodeMessage(
                      src.getCurrentId(),
                      src.getLevel(),
                      src.getHead(),
                      src.getPath(),
                      true,
                      src.isLeaf())));
        }
      } else { // Regular reporting structure
        if (src.isLeaf()) { // Update source leaf
          messages.$plus$eq(
              new scala.Tuple2(
                  triplet.srcId(),
                  new EventNodeMessage(
                      src.getCurrentId(),
                      src.getLevel(),
                      src.getHead(),
                      src.getPath(),
                      false,
                      false // important value
                      )));
        } else { // propagate values to destination
          messages.$plus$eq(
              new scala.Tuple2(
                  triplet.dstId(),
                  new EventNodeMessage(
                      src.getCurrentId(),
                      src.getLevel(),
                      src.getHead(),
                      src.getPath(),
                      false,
                      true // important for leaf updating
                      )));
        }
      }

      return messages.iterator();
    }
  }

  public static class MergeMsg
      implements scala.Function2<EventNodeMessage, EventNodeMessage, EventNodeMessage>,
          Serializable {

    @Override
    public EventNodeMessage apply(EventNodeMessage msg1, EventNodeMessage msg2) {
      return msg2;
    }
  }

  public static class EventNodeMapper extends AbstractFunction2<Object, EventNode, EventNodeValue>
      implements java.io.Serializable {
    @Override
    public EventNodeValue apply(Object id, EventNode v) {
      long vertexId = ((Number) id).longValue();
      return new EventNodeValue(
          v.eventId, vertexId, 0, v.eventId, Arrays.asList(v.eventId), false, false);
    }
  }

  static class EventNode implements Serializable {

    public String eventId;
    public String parentEventId;

    public EventNode() {}

    public EventNode(String eventId, String parentEventId) {
      this.eventId = eventId;
      this.parentEventId = parentEventId;
    }

    public String getEventId() {
      return eventId;
    }

    public void setEventId(String eventId) {
      this.eventId = eventId;
    }

    public String getParentEventId() {
      return parentEventId;
    }

    public void setParentEventId(String parentEventId) {
      this.parentEventId = parentEventId;
    }
  }

  // Inner class representing the structure of the message to be passed to vertices
  public static class EventNodeMessage implements Serializable {
    private final long
        currentId; // Tracks the most recent vertex appended to path and used for flagging isCyclic
    private final int level; // The number of up-line supervisors (level in reporting hierarchy)
    private final String head; // The top-most supervisor
    private final List<String> path; // The reporting path to the top-most supervisor
    private final boolean isCyclic; // Is the reporting structure of the event cyclic
    private final boolean isLeaf; // Is the event rank and file (no down-line reporting event)

    public EventNodeMessage(
        long currentId,
        int level,
        String head,
        List<String> path,
        boolean isCyclic,
        boolean isLeaf) {
      this.currentId = currentId;
      this.level = level;
      this.head = head;
      this.path = path;
      this.isCyclic = isCyclic;
      this.isLeaf = isLeaf;
    }

    // Getters
    public long getCurrentId() {
      return currentId;
    }

    public int getLevel() {
      return level;
    }

    public String getHead() {
      return head;
    }

    public List<String> getPath() {
      return path;
    }

    public boolean isCyclic() {
      return isCyclic;
    }

    public boolean isLeaf() {
      return isLeaf;
    }
  }

  // Inner class representing the structure of the vertex values of the graph
  public static class EventNodeValue implements Serializable {
    private final String name; // The event name
    private final long currentId; // Initial value is the eventId
    private final int level; // Initial value is zero
    private final String head; // Initial value is this eventNode's Id
    private final List<String> path; // Initial value contains this eventNode's Id only
    private final boolean isCyclic; // Initial value is false
    private final boolean isLeaf; // Initial value is true

    public EventNodeValue(
        String name,
        long currentId,
        int level,
        String head,
        List<String> path,
        boolean isCyclic,
        boolean isLeaf) {
      this.name = name;
      this.currentId = currentId;
      this.level = level;
      this.head = head;
      this.path = path;
      this.isCyclic = isCyclic;
      this.isLeaf = isLeaf;
    }

    // Getters
    public String getName() {
      return name;
    }

    public long getCurrentId() {
      return currentId;
    }

    public int getLevel() {
      return level;
    }

    public String getHead() {
      return head;
    }

    public List<String> getPath() {
      return path;
    }

    public boolean isCyclic() {
      return isCyclic;
    }

    public boolean isLeaf() {
      return isLeaf;
    }
  }
}
