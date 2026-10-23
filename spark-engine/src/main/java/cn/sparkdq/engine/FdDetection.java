package cn.sparkdq.engine;

import java.util.*;
import org.apache.spark.sql.*;
import static org.apache.spark.sql.functions.*;

/** Validate configured X -> Y using actual tuples, retaining every conflicting participant. */
final class FdDetection {
 record Result(Map<String,Object> metrics,Dataset<Row> anomalies){}
 static Result detect(Dataset<Row> df,List<String> x,List<String> y,long total){
  var all=new ArrayList<>(x);all.addAll(y);
  if(x.isEmpty()||y.isEmpty()||x.size()>16||y.size()>16||new HashSet<>(all).size()!=all.size()||!Arrays.asList(df.columns()).containsAll(all))throw new IllegalArgumentException("Invalid FD X/Y columns");
  Column blank=lit(false),nulls=lit(false),empty=lit(false),space=lit(false);
  for(String name:all){
   Column c=col(name);nulls=nulls.or(c.isNull());empty=empty.or(c.isNotNull().and(c.equalTo("")));
   space=space.or(c.isNotNull().and(length(c).gt(0)).and(length(trim(c)).equalTo(0)));
   blank=blank.or(c.isNull().or(length(trim(c)).equalTo(0)));
  }
  Dataset<Row> eligible=df.filter(not(blank));
  Column[] keys=x.stream().map(org.apache.spark.sql.functions::col).toArray(Column[]::new);
  Column[] values=y.stream().map(org.apache.spark.sql.functions::col).toArray(Column[]::new);
  Dataset<Row> groups=eligible.groupBy(keys).agg(countDistinct(struct(values)).alias("__dq_fd_values")).cache();
  try{
   long checked=eligible.count(),checkedGroups=groups.count();Dataset<Row> conflict=groups.filter(col("__dq_fd_values").gt(1));long conflicts=conflict.count();
   // Semi-join keeps duplicates and all original fields; no string concatenation, majority vote or Driver collect.
   Dataset<Row> bad=eligible.join(conflict.select(keys),x.toArray(String[]::new),"left_semi");long count=bad.count();
   var m=new LinkedHashMap<String,Object>();m.put("algorithmVersion","fd-exact-text-skip-blank-v1");m.put("status",checked==0?"NO_VALID_ROWS":"EVALUATED");
   m.put("checkedCount",checked);m.put("skippedCount",total-checked);m.put("skippedBlankCount",total-checked);
   m.put("skippedNullCount",Engine.sum(df,nulls));m.put("skippedEmptyCount",Engine.sum(df,empty));m.put("skippedWhitespaceCount",Engine.sum(df,space));
   m.put("checkedGroups",checkedGroups);m.put("conflictGroups",conflicts);m.put("conflictRows",count);m.put("invalidCount",count);
   return new Result(m,bad);
  }finally{groups.unpersist();}
 }
}
