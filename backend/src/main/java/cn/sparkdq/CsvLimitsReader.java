package cn.sparkdq;
import java.io.*;
import java.util.*;

/** Streaming allocation guard; Commons CSV remains responsible for syntax and values. */
final class CsvLimitsReader extends FilterReader {
 static final int CELL=65536,COLUMNS=100,PREVIEW=256,RAW_FIELD=2*CELL+2;
 final long deadline;int chars,raw,columns=1;boolean start=true,quoted,pending,afterQuote;
 CsvLimitsReader(Reader source,long deadline){super(source);this.deadline=deadline;}
 void character(int c){
  if(c==0)throw Datasets.bad("单元格包含 NUL 或长度超限");
  if(quoted&&pending){if(c=='"'){pending=false;increment(c,true);return;}quoted=false;pending=false;afterQuote=true;}
  if(!quoted&&(c==','||c=='\r'||c=='\n')){
   if(c==','){if(++columns>COLUMNS)throw Datasets.bad("每条 CSV 记录最多100列");}else columns=1;
   chars=0;raw=0;start=true;afterQuote=false;return;
  }
  if(quoted){if(c=='"'){pending=true;increment(c,false);}else increment(c,true);return;}
  if(start&&c=='"'){quoted=true;start=false;increment(c,false);return;}
  start=false;increment(c,!(afterQuote&&Character.isWhitespace(c)));
 }
 void increment(int c,boolean logical){if(++raw>RAW_FIELD||(logical&&++chars>CELL))throw Datasets.bad("单元格长度或原始字段长度超限");}
 void check(){if(deadline!=Long.MAX_VALUE)ControlledProfile.checkpoint(deadline);}
 @Override public int read()throws IOException{check();int c=super.read();if(c!=-1)character(c);return c;}
 @Override public int read(char[] buffer,int offset,int length)throws IOException{check();int n=super.read(buffer,offset,Math.min(length,8192));for(int i=0;i<n;i++)character(buffer[offset+i]);return n;}
 static String preview(String value){if(value==null||value.length()<=PREVIEW)return value;int end=PREVIEW-1;if(Character.isHighSurrogate(value.charAt(end-1)))end--;return value.substring(0,end)+"…";}
 static List<List<String>> previewRows(Object value){
  if(!(value instanceof List<?> rows))throw new IllegalStateException("预览结构异常");var result=new ArrayList<List<String>>();
  for(var item:rows.subList(0,Math.min(10,rows.size()))){if(!(item instanceof List<?> row))throw new IllegalStateException("预览结构异常");result.add(row.stream().limit(COLUMNS).map(v->preview(v==null?null:v.toString())).toList());}return result;
 }
 static Object previewObject(Object value){
  if(!(value instanceof Map<?,?> row)||row.size()>COLUMNS+1)throw new IllegalStateException("异常预览结构超限");var result=new LinkedHashMap<String,Object>();
  row.forEach((key,cell)->result.put(key.toString(),cell instanceof String text?preview(text):cell));return result;
 }
}
