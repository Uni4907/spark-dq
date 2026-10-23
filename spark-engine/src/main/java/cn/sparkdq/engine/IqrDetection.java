package cn.sparkdq.engine;

import java.math.BigDecimal;
import java.util.*;
import java.util.regex.Pattern;
import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import static org.apache.spark.sql.functions.*;

/** Exact decimal fences. Only the <=4 rank values, never the whole input, reach the Driver. */
final class IqrDetection {
 static final String DOMAIN="\\A[+-]?(?:[0-9]{1,28}(?:\\.[0-9]{1,10})?|\\.[0-9]{1,10})\\z";
 static final String SYNTAX="\\A[+-]?(?:[0-9]+(?:\\.[0-9]+)?|\\.[0-9]+)\\z";
 static final Pattern INPUT=Pattern.compile(DOMAIN);
 record Result(Map<String,Object> metrics,Dataset<Row> anomalies){}
 static BigDecimal interpolate(BigDecimal low,BigDecimal high,int remainder){
  return low.add(high.subtract(low).multiply(BigDecimal.valueOf(remainder)).divide(BigDecimal.valueOf(4)));
 }
 static String plain(BigDecimal n){return n.stripTrailingZeros().toPlainString();}
 static boolean outside(String raw,BigDecimal lower,BigDecimal upper){
  if(raw==null||!INPUT.matcher(raw).matches())return false;
  BigDecimal n=new BigDecimal(raw);return n.compareTo(lower)<0||n.compareTo(upper)>0;
 }
 static BigDecimal multiplier(String text){
  if(text==null||text.length()>16||!text.matches("[0-9]+(?:\\.[0-9]{1,4})?"))throw new IllegalArgumentException("IQR k must be a decimal in (0,10], up to 4 decimal places");
  BigDecimal k=new BigDecimal(text);if(k.signum()<=0||k.compareTo(BigDecimal.TEN)>0)throw new IllegalArgumentException("IQR k outside (0,10]");return k;
 }
 static Result detect(Dataset<Row> df,String name,String kText,long total){
  BigDecimal k=multiplier(kText);Column raw=col(name),blank=raw.isNull().or(length(trim(raw)).equalTo(0));
  Column syntax=coalesce(raw.rlike(SYNTAX),lit(false));
  Column numeric=coalesce(raw.rlike(DOMAIN),lit(false));
  // Regex bounds ensure cast is lossless: at most 28 integer digits and 10 fractional digits.
  Dataset<Row> valid=df.filter(numeric).select(raw.cast(DataTypes.createDecimalType(38,10)).alias("__dq_iqr_value")).cache();
  try{
   long n=valid.count(),blankCount=Engine.sum(df,blank),nonNumeric=Engine.sum(df,not(blank).and(not(syntax)));
   long outOfDomain=total-n-blankCount-nonNumeric;
   var m=new LinkedHashMap<String,Object>();
   m.put("algorithmVersion","iqr-exact-decimal-linear-v1");m.put("status",n<4?"INSUFFICIENT_DATA":"EVALUATED");
   m.put("numericDomain","DECIMAL_28_10_NO_EXPONENT_NO_TRIM");m.put("minimumValidCount",4);m.put("k",plain(k));
   m.put("checkedCount",n);m.put("evaluatedCount",n<4?0L:n);m.put("skippedCount",total-n);
   m.put("skippedBlankCount",blankCount);m.put("nonNumericCount",nonNumeric);m.put("outOfDomainCount",outOfDomain);
   for(String field:List.of("q1","q3","iqr","lowerBound","upperBound"))m.put(field,null);
   Dataset<Row> bad=df.filter(lit(false));
   if(n>=4){
    long a=(n-1)/4,b=Math.multiplyExact(n-1,3)/4;
    int ar=(int)((n-1)%4),br=(int)(Math.multiplyExact(n-1,3)%4);
    var ranks=new HashSet<Long>(List.of(a,a+(ar==0?0:1),b,b+(br==0?0:1)));
    var points=valid.orderBy(col("__dq_iqr_value")).javaRDD().zipWithIndex().filter(t->ranks.contains(t._2())).collect();
    var values=new HashMap<Long,BigDecimal>();for(var point:points)values.put(point._2(),point._1().getDecimal(0));
    BigDecimal q1=interpolate(values.get(a),values.get(a+(ar==0?0:1)),ar),q3=interpolate(values.get(b),values.get(b+(br==0?0:1)),br);
    BigDecimal iqr=q3.subtract(q1),lower=q1.subtract(k.multiply(iqr)),upper=q3.add(k.multiply(iqr));
    m.put("q1",plain(q1));m.put("q3",plain(q3));m.put("iqr",plain(iqr));m.put("lowerBound",plain(lower));m.put("upperBound",plain(upper));
    if(iqr.signum()==0)m.put("status","ZERO_IQR");
    // Java BigDecimal comparison avoids Spark decimal arithmetic precision reduction for derived fences.
    bad=df.filter(udf((UDF1<String,Boolean>)(value->outside(value,lower,upper)),DataTypes.BooleanType).apply(raw));
   }
   m.put("invalidCount",bad.count());return new Result(m,bad);
  }finally{valid.unpersist();}
 }
}
