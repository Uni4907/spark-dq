package cn.sparkdq;
import org.springframework.web.bind.annotation.*;
import java.util.*;
/** Operator-neutral status: no endpoints, secrets or process commands. */
@RestController
public class ExecutionStatusApi {
 final Tasks tasks;
 public ExecutionStatusApi(Tasks tasks){this.tasks=tasks;}
 @GetMapping("/api/execution/status") public Map<String,Object> status(){
  int pending=tasks.recoveryPending.size()+(int)tasks.jobs.values().stream().filter(job->(job.runnerFinished||job.cancelled||job.failureReason!=null)&&tasks.cluster.hasIntent(tasks.taskDirectory(job.id))).count();
  return Map.of("mode",tasks.cluster.profile.remote()?"standalone":"local","cleanupPending",pending,"acceptingTasks",!tasks.pool.isShutdown()&&pending==0);
 }
}
