package cn.sparkdq;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.Semaphore;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;

/** A separate bounded product profile, never inferred from request size or a path. */
final class ControlledProfile {
 static final long MAX_BYTES=64L*1024*1024,MAX_ROWS=1_000_000,MAX_OUTPUT=1024L*1024*1024,MIN_FREE=8L*1024*1024*1024;
 static final Semaphore REGISTRATION=new Semaphore(1);
 static boolean controlled(Map<String,Object> data){return data.get("source") instanceof Map<?,?> source&&"CONTROLLED_CSV".equals(source.get("format"))&&"CONTROLLED_64M_V1".equals(source.get("profile"));}
 static void checkpoint(long deadline){if(Thread.currentThread().isInterrupted()||System.nanoTime()>deadline)throw Datasets.bad("受控文件登记超过120秒或被中断，未发布数据集");}
 static long treeBytes(Path directory,long ceiling,int maxEntries)throws Exception{
  if(!Files.exists(directory))return 0;long[] bytes={0};int[] entries={0};
  Files.walkFileTree(directory,new SimpleFileVisitor<Path>(){
   void entry(java.nio.file.attribute.BasicFileAttributes attrs){if(++entries[0]>maxEntries||attrs.isSymbolicLink())throw new IllegalStateException("受控数据目录超限或含链接");}
   @Override public FileVisitResult preVisitDirectory(Path path,java.nio.file.attribute.BasicFileAttributes attrs){entry(attrs);return FileVisitResult.CONTINUE;}
   @Override public FileVisitResult visitFile(Path path,java.nio.file.attribute.BasicFileAttributes attrs){entry(attrs);if(attrs.isRegularFile()){bytes[0]=Math.addExact(bytes[0],attrs.size());if(bytes[0]>ceiling)throw new IllegalStateException("受控数据文件预算超限");}return FileVisitResult.CONTINUE;}
   @Override public FileVisitResult visitFileFailed(Path path,java.io.IOException error)throws java.io.IOException{if(error instanceof NoSuchFileException)return FileVisitResult.CONTINUE;throw error;}
   @Override public FileVisitResult postVisitDirectory(Path path,java.io.IOException error)throws java.io.IOException{if(error!=null&&!(error instanceof NoSuchFileException))throw error;return FileVisitResult.CONTINUE;}
  });
  return bytes[0];
 }
 static void admit(Path root)throws Exception{
  Files.createDirectories(root.resolve("raw"));
  if(Files.getFileStore(root).getUsableSpace()<MIN_FREE||treeBytes(root.resolve("raw"),MAX_OUTPUT,10000)+MAX_BYTES>MAX_OUTPUT)throw new ResponseStatusException(HttpStatus.INSUFFICIENT_STORAGE,"受控文件要求至少8GiB可用空间，原始数据保留预算1GiB");
 }
 static void output(Path directory)throws Exception{
  if(Files.getFileStore(directory).getUsableSpace()<MIN_FREE)throw new IllegalStateException("受控任务可用空间不足8GiB，停止计算");
  treeBytes(directory,MAX_OUTPUT,10000);
 }
}
