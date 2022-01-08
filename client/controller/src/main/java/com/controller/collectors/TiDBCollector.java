/*
 * OtterTune - MySQLCollector.java
 *
 * Copyright (c) 2017-18, Carnegie Mellon University Database Group
 */

package com.controller.collectors;

import com.controller.util.JSONUtil;
import com.controller.util.json.JSONException;
import com.controller.util.json.JSONObject;
import com.controller.util.json.JSONStringer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.TreeMap;
import java.util.HashMap;
import org.apache.log4j.Logger;

/** */
public class TiDBCollector extends DBCollector {
  private static final Logger LOG = Logger.getLogger(MySQLCollector.class);

  private static final String VERSION_SQL = "SELECT @@GLOBAL.version;";

  // Write SQL to collect parameters
  private static final String PARAMETERS_SQL = "SHOW CONFIG;";
  private static final String VARIABLES_SQL = "SHOW GLOBAL VARIABLES;";

  // Write SQL to get metrics
  private static final Map<String, String> METRICS_SQL = new TreeMap<String, String>(){
		{
			put("tidb_qps",
			"SELECT instance,sum(value)/7 'sum(value)' from METRICS_SCHEMA.tidb_qps where result='OK' and time between now()-interval 4 minute and now()-interval 1 minute group by instance;");
			put("tidb_query_duration",
			"SELECT instance,sum(value)/7 'sum(value)' from METRICS_SCHEMA.tidb_query_duration where quantile=0.99 and time between now()-interval 4 minute and now()-interval 1 minute group by instance;");
			put("tikv_grpc_message_total_count",
			"SELECT instance,sum(value)/7 'sum(value)' from METRICS_SCHEMA.tikv_grpc_message_total_count where time between now()-interval 4 minute and now()-interval 1 minute group by instance;");
			put("tikv_grpc_message_duration",
			"SELECT instance,sum(value)/7 'sum(value)' from METRICS_SCHEMA.tikv_grpc_message_duration where time between now()-interval 4 minute and now()-interval 1 minute group by instance;");
			put("tikv_thread_cpu",
			"SELECT instance,sum(value)/7 'sum(value)' from METRICS_SCHEMA.tikv_thread_cpu where time between now()-interval 4 minute and now()-interval 1 minute group by instance;");
			put("tikv_memory",
			"SELECT instance,sum(value)/7 'sum(value)' from METRICS_SCHEMA.tikv_memory where time between now()-interval 4 minute and now()-interval 1 minute group by instance;");
			put("node_disk_io_util",
			"SELECT instance,sum(value)/7 'sum(value)' from METRICS_SCHEMA.node_disk_io_util where device='vdc' and time between now()-interval 4 minute and now()-interval 1 minute group by instance;");
		}
  };

  public TiDBCollector(String oriDBUrl, String username, String password) {
    try {
      Connection conn = DriverManager.getConnection(oriDBUrl, username, password);
      Statement s = conn.createStatement();

      // Set tidb_metric_query_range_duration and tidb_metric_query_step to 30 sec
      s.executeQuery("set@@tidb_metric_query_range_duration=30;");
      s.executeQuery("set@@tidb_metric_query_step=30;");

      // Collect DBMS version
      ResultSet out = s.executeQuery(VERSION_SQL);
      if (out.next()) {
        this.version.append(out.getString(1));
      }

      // Collect DBMS parameters
      out = s.executeQuery(PARAMETERS_SQL);
      while (out.next()) {
        dbParameters.put(
            "("
                + out.getString("Type")
                + ","
                + out.getString("Instance")
                + ","
                + out.getString("Name")
                + ")",
            out.getString("Value"));
      }
      out = s.executeQuery(VARIABLES_SQL);
      while (out.next()) {
        dbParameters.put(
            "("
                + out.getString("Variable_name")
                + ")",
            out.getString("Value"));
      }

      // Collect DBMS internal metrics: ((name,instance), sum(value))
      for (Map.Entry<String, String> entry : METRICS_SQL.entrySet()) {
		LOG.info("SQL: " + entry.getValue());
        out = s.executeQuery(entry.getValue());
        while (out.next()) {
            dbMetrics.put(
                "("
                    + entry.getKey()
                    + ","
                    + out.getString("instance")
                    + ")",
                out.getString("sum(value)"));
        }
      }

      conn.close();
    } catch (SQLException e) {
      LOG.error("Error while collecting DB parameters: " + e.getMessage());
      e.printStackTrace();
    }
  }

  @Override
  public String collectParameters() {
    JSONStringer stringer = new JSONStringer();
    try {
      stringer.object();
      stringer.key(JSON_GLOBAL_KEY);
      JSONObject jobLocal = new JSONObject();
      JSONObject job = new JSONObject();
      for (String k : dbParameters.keySet()) {
        job.put(k, dbParameters.get(k));
      }
      // "global is a fake view_name (a placeholder)"
      jobLocal.put("global", job);
      stringer.value(jobLocal);
      stringer.key(JSON_LOCAL_KEY);
      stringer.value(null);
      stringer.endObject();
    } catch (JSONException jsonexn) {
      jsonexn.printStackTrace();
    }
    return JSONUtil.format(stringer.toString());
  }

  @Override
  public String collectMetrics() {
    JSONStringer stringer = new JSONStringer();
    try {
      stringer.object();
      stringer.key(JSON_GLOBAL_KEY);
      JSONObject jobGlobal = new JSONObject();
      JSONObject job = new JSONObject();
      for (Map.Entry<String, String> entry : dbMetrics.entrySet()) {
        job.put(entry.getKey(), entry.getValue());
      }
      // "global is a fake view_name (a placeholder)"
      jobGlobal.put("global", job);
      stringer.value(jobGlobal);
      stringer.key(JSON_LOCAL_KEY);
      stringer.value(null);
      stringer.endObject();
    } catch (JSONException e) {
      e.printStackTrace();
    }
    return JSONUtil.format(stringer.toString());
  }
}
