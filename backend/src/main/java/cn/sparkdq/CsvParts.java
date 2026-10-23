package cn.sparkdq;

import org.apache.commons.csv.*;
import org.springframework.web.multipart.MultipartFile;
import java.io.*;
import java.nio.charset.*;
import java.nio.file.*;
import java.util.*;

/** Bounded registration of an ordered set of local CSV uploads. No client paths. */
final class CsvParts {
 static final long LIMIT=20L*1024*1024;
 record Merged(Path csv,List<Map<String,Object>> files,List<String> columns,long rows){}
 static Merged merge(List<MultipartFile> uploads,Path root)throws Exception {
  if(uploads==null||uploads.size()<2||uploads.size()>16)throw Datasets.bad("分片注册须选择 2..16 份 CSV");
  var names=new HashSet<String>();var safeNames=new ArrayList<String>();
  for(var upload:uploads){
   String name=upload.getOriginalFilename();
   if(name==null)throw Datasets.bad("分片须有 CSV 文件名");
   name=name.replace('\\','/');name=name.substring(name.lastIndexOf('/')+1);
   if(name.length()>200||!name.toLowerCase(Locale.ROOT).endsWith(".csv")||!names.add(name)||name.chars().anyMatch(Character::isISOControl))throw Datasets.bad("分片名称须唯一、无控制字符且以 .csv 结尾（最多200字符）");
   if(upload.isEmpty())throw Datasets.bad("分片不能是空文件");
   safeNames.add(name);
  }
  Files.createDirectory(root);Path combined=root.resolve("combined.pending.csv");
  var files=new ArrayList<Map<String,Object>>();List<String> columns=null;long bytes=0,rows=0;
  try(var stream=Files.newOutputStream(combined,StandardOpenOption.CREATE_NEW);
      var bounded=new FilterOutputStream(stream){long size;@Override public void write(int b)throws IOException{if(++size>LIMIT)throw new IOException("合并文件不能超过20MiB");out.write(b);}
       @Override public void write(byte[] b,int off,int len)throws IOException{if((size+=len)>LIMIT)throw new IOException("合并文件不能超过20MiB");out.write(b,off,len);}};
      var printer=new CSVPrinter(new OutputStreamWriter(bounded,StandardCharsets.UTF_8),CSVFormat.RFC4180.builder().setQuoteMode(QuoteMode.ALL).get())){
   for(int index=0;index<uploads.size();index++){
    Path part=root.resolve(String.format(Locale.ROOT,"%03d.csv",index+1));
    try(var input=uploads.get(index).getInputStream();var output=Files.newOutputStream(part,StandardOpenOption.CREATE_NEW)){
     byte[] block=new byte[8192];int n;while((n=input.read(block))!=-1){bytes+=n;if(bytes>LIMIT)throw Datasets.bad("分片原文件总量不能超过20MiB");output.write(block,0,n);}
    }
    long count=0;
    var decoder=StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT).onUnmappableCharacter(CodingErrorAction.REPORT);
    try(var reader=new PushbackReader(new InputStreamReader(Files.newInputStream(part),decoder),1)){
     int first=reader.read();if(first!=-1&&first!=0xfeff)reader.unread(first);
     try(var parser=CSVFormat.RFC4180.builder().setIgnoreEmptyLines(true).get().parse(new CsvLimitsReader(reader,Long.MAX_VALUE))){
      var it=parser.iterator();if(!it.hasNext())throw Datasets.bad("分片缺少表头");var header=it.next().toList();
      if(header.isEmpty()||header.size()>100||new HashSet<>(header).size()!=header.size()||header.stream().anyMatch(h->!h.matches("[A-Za-z][A-Za-z0-9_]{0,63}")))throw Datasets.bad("分片字段须唯一，英文字母开头，最多64字符/100列");
      if(columns==null){columns=List.copyOf(header);printer.printRecord(columns);}else if(!columns.equals(header))throw Datasets.bad("所有分片的字段及顺序必须完全一致");
      while(it.hasNext()){
       var row=it.next();if(row.size()!=columns.size())throw Datasets.bad("分片记录列数不匹配");
       for(String value:row)if(value.indexOf(0)>=0||value.length()>65536)throw Datasets.bad("单元格包含NUL或长度超限");
       printer.printRecord(row);count++;rows++;
      }
     }
    }
    part.toFile().setReadOnly();
    files.add(Map.of("path",part.getFileName().toString(),"name",safeNames.get(index),"size",Files.size(part),"sha256",ExecutionInputs.sha(part),"rows",count));
   }
  }catch(org.springframework.web.server.ResponseStatusException e){throw e;}
   catch(IOException|UncheckedIOException e){throw Datasets.bad("分片须为UTF-8/RFC4180 CSV，且原文件总量与合并文件均不超过20MiB");}
  if(rows==0)throw Datasets.bad("合并数据至少需要一条记录");
  return new Merged(combined,List.copyOf(files),columns,rows);
 }
}
