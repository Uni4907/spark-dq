package cn.sparkdq.engine;
import java.math.BigDecimal;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
class IqrDetectionTest {
 @Test void exactInterpolationBeyondDoubleIntegerPrecision(){
  assertEquals("9007199254740993.75",IqrDetection.plain(IqrDetection.interpolate(new BigDecimal("9007199254740993"),new BigDecimal("9007199254740994"),3)));
 }
 @Test void strictFencesAndTinyFractions(){
  var lo=new BigDecimal("-4.5");var hi=new BigDecimal("9.5");
  assertFalse(IqrDetection.outside("-4.5",lo,hi));assertFalse(IqrDetection.outside("9.5",lo,hi));
  assertTrue(IqrDetection.outside("9.5000000001",lo,hi));assertTrue(IqrDetection.outside("-4.5000000001",lo,hi));
  assertFalse(IqrDetection.outside(" 10",lo,hi));assertFalse(IqrDetection.outside("NaN",lo,hi));
 }
 @Test void javaRegexFindRejectsTrailingNewlinesAndWhitespace(){
  for(String raw:java.util.List.of("1\n","1\r\n","1 "," 1","1\t"))assertFalse(IqrDetection.INPUT.matcher(raw).find(),raw);
  assertTrue(IqrDetection.INPUT.matcher("1.0").find());
 }
 @Test void domainAndMultiplierAreBounded(){
  assertTrue(IqrDetection.INPUT.matcher("9999999999999999999999999999.1234567890").matches());
  assertFalse(IqrDetection.INPUT.matcher("10000000000000000000000000000").matches());
  assertThrows(IllegalArgumentException.class,()->IqrDetection.multiplier("1e0"));
 }
}
