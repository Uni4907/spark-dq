package cn.sparkdq;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.sql.init.dependency.DependsOnDatabaseInitialization;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

/** Admission gate shared by ALL mutating HTTP requests and maintenance entry. */
@Service @DependsOnDatabaseInitialization
public class Maintenance {
 static final String KEY_PATH="/tmp/sparkdq-maintenance.key";
 final JdbcTemplate db;final Path marker;final String key;
 final ReentrantReadWriteLock gate=new ReentrantReadWriteLock(true);
 volatile String owner;
 @Autowired public Maintenance(JdbcTemplate db)throws IOException {
  this(db,Path.of("/tmp/sparkdq-maintenance.owner"),Path.of(KEY_PATH));
 }
 Maintenance(JdbcTemplate db,Path marker,Path keyPath)throws IOException {
  this.db=db;this.marker=marker;
  if(Files.exists(marker))owner=validOwner(Files.readString(marker).trim());
  byte[] secret=new byte[32];new SecureRandom().nextBytes(secret);
  key=HexFormat.of().formatHex(secret);
  atomicPrivate(keyPath,key);
 }
 static String validOwner(String value) {
  try {
   if(value==null||!UUID.fromString(value).toString().equals(value))throw new IllegalArgumentException();
   return value;
  } catch(IllegalArgumentException error) {throw new ResponseStatusException(HttpStatus.BAD_REQUEST,"维护会话标识无效");}
 }
 static void atomicPrivate(Path target,String value)throws IOException {
  Path temporary=Files.createTempFile(target.getParent(),".sparkdq-maintenance-",null,
    PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------")));
  try {
   Files.writeString(temporary,value,StandardCharsets.UTF_8);
   Files.move(temporary,target,StandardCopyOption.ATOMIC_MOVE,StandardCopyOption.REPLACE_EXISTING);
  } finally {Files.deleteIfExists(temporary);}
 }
 void authorize(String candidate) {
  if(candidate==null||!MessageDigest.isEqual(key.getBytes(StandardCharsets.US_ASCII),candidate.getBytes(StandardCharsets.UTF_8)))
   throw new ResponseStatusException(HttpStatus.FORBIDDEN,"维护操作未授权");
 }
 boolean enabled(){return owner!=null;}
 Map<String,Object> state(){String current=owner;return Map.of("maintenance",current!=null,"owner",current==null?"":current);}
 void lock() {
  try {
   if(!gate.writeLock().tryLock(10,TimeUnit.SECONDS))
    throw new ResponseStatusException(HttpStatus.CONFLICT,"仍有写入请求正在处理，请稍后维护");
  } catch(InterruptedException error) {
   Thread.currentThread().interrupt();throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE,"维护请求被中断");
  }
 }
 Map<String,Object> enter(String requestOwner)throws IOException {
  String requested=validOwner(requestOwner);lock();
  try {
   if(owner!=null) {
    if(!owner.equals(requested))throw new ResponseStatusException(HttpStatus.CONFLICT,"已有其他维护会话");
    return state();
   }
   Long active=db.queryForObject("select (select count(*) from tasks where status in ('QUEUED','RUNNING','CANCELLING'))+(select count(*) from dataset_imports where status in ('QUEUED','RUNNING','CANCELLING'))",Long.class);
   if(active==null||active!=0)throw new ResponseStatusException(HttpStatus.CONFLICT,"存在活动任务，未进入维护");
   atomicPrivate(marker,requested);owner=requested;
   return state();
  } finally {gate.writeLock().unlock();}
 }
 Map<String,Object> leave(String requestOwner)throws IOException {
  String requested=validOwner(requestOwner);lock();
  try {
   if(owner!=null&&!owner.equals(requested))throw new ResponseStatusException(HttpStatus.CONFLICT,"维护会话不匹配，未解除保护");
   Files.deleteIfExists(marker);owner=null;
   return state();
  } finally {gate.writeLock().unlock();}
 }
}
