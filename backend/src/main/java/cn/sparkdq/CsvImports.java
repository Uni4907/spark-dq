package cn.sparkdq;
import java.util.*;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.transaction.annotation.Transactional;

/** Isolates the transaction proxy from the legacy Datasets shared field access. */
@Service
public class CsvImports {
 private final Datasets datasets;
 public CsvImports(Datasets datasets){this.datasets=datasets;}
 @Transactional(rollbackFor=Exception.class)
 public Map<String,Object> upload(List<MultipartFile> files)throws Exception{return datasets.uploadParts(files);}
}
