package cn.sparkdq;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.http.ResponseEntity;
import org.springframework.web.multipart.MaxUploadSizeExceededException;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.MissingServletRequestParameterException;
import org.springframework.web.multipart.support.MissingServletRequestPartException;
import java.util.Map;
@RestControllerAdvice
public class Errors {
 @ExceptionHandler(ResponseStatusException.class) ResponseEntity<?> status(ResponseStatusException e){return ResponseEntity.status(e.getStatusCode()).body(Map.of("message",e.getReason()==null?"请求失败":e.getReason()));}
 @ExceptionHandler(MaxUploadSizeExceededException.class) ResponseEntity<?> large(){return ResponseEntity.status(413).body(Map.of("message","文件不能超过 20 MiB"));}
 @ExceptionHandler({HttpMessageNotReadableException.class,MissingServletRequestParameterException.class,MissingServletRequestPartException.class}) ResponseEntity<?> invalid(){return ResponseEntity.badRequest().body(Map.of("message","请求参数缺失或格式错误"));}
}
