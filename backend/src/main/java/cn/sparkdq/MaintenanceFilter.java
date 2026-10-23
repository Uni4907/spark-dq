package cn.sparkdq;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.servlet.*;
import jakarta.servlet.http.*;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

@Component @Order(Ordered.HIGHEST_PRECEDENCE)
public class MaintenanceFilter extends OncePerRequestFilter {
 final Maintenance maintenance;final ObjectMapper json;
 public MaintenanceFilter(Maintenance maintenance,ObjectMapper json){this.maintenance=maintenance;this.json=json;}
 boolean readOnly(HttpServletRequest request) {
  return switch(request.getMethod()){case "GET","HEAD","OPTIONS"->true;default->false;};
 }
 void unavailable(HttpServletResponse response)throws IOException {
  response.setStatus(503);response.setContentType("application/json;charset=UTF-8");response.setHeader("Retry-After","30");
  json.writeValue(response.getOutputStream(),Map.of("message","系统维护中，暂不接受写入；请稍后重试"));
 }
 @Override protected void doFilterInternal(HttpServletRequest request,HttpServletResponse response,FilterChain chain)throws ServletException,IOException {
  String path=request.getRequestURI();
  // Only authenticated maintenance controller routes bypass the mutation gate.
  if(readOnly(request)||path.equals("/api/internal/maintenance/enter")||path.equals("/api/internal/maintenance/leave")) {
   chain.doFilter(request,response);return;
  }
  boolean held=false;
  try {
   held=maintenance.gate.readLock().tryLock(1,TimeUnit.SECONDS);
   if(!held||maintenance.enabled()){unavailable(response);return;}
   chain.doFilter(request,response);
  } catch(InterruptedException error) {Thread.currentThread().interrupt();unavailable(response);}
  finally {if(held)maintenance.gate.readLock().unlock();}
 }
}
