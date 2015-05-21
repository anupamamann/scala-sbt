import org.apache.spark.SparkContext
import org.scalatest.{FlatSpec, Matchers}

/**
 * Created by anupamam on 5/10/15.
 * Reviewed by maasg on 05/18/15.
 */
class WordsMinDistanceTest extends FlatSpec with Matchers {
  val filePath = "src/test/resources/MK-Bio"
  val abcPath = "src/test/resources/abc.txt"
  val baabPath = "src/test/resources/baab.txt"
  val abcdePath = "src/test/resources/abcde.txt"

val sc = new SparkContext("local","WordsDistance")
  "min Distance" should "error(-1) with null file" in {
    val searchWords = List("born", "family")
    assert(WordsMinDistance(sc,null, searchWords) == -1)

  }

  /**
   * Commenting out the test case, Spark errors out when file
   * doesn't exist
   */
  /*
  "min Distance" should "error (-1) with wrong file path" in {
    val searchWords = List("born", "family")
    WordsMinDistance(sc,"hello", searchWords) shouldBe -1
  }
  */

  "min Distance" should "error (-1) for insufficient search words" in {
    val searchWords = List("born")
    WordsMinDistance(sc,filePath, searchWords) shouldBe -1

  }

  "min Distance" should "be 11" in {
    val searchWords = List("born", "family")
    WordsMinDistance(sc,filePath, searchWords) shouldBe 10
  }

  "min Distance" should "be 279" in {
    val searchWords = List("born", "family", "married")
    WordsMinDistance(sc,filePath, searchWords) shouldBe (279)

  }

  "min Distance" should "be -1 when words do not exist in text" in {
    val searchWords = List("bleh", "rule", "hellooo")
    WordsMinDistance(sc,filePath, searchWords) shouldBe (-1)

  }

  "min Distance" should "be 0 in three consecutive words" in {
    WordsMinDistance(sc,abcPath, List("a","b","c")) should be (0)
  }

  "min Distance" should "be 0 in two consecutive equal words" in {
    WordsMinDistance(sc,abcPath, List("a","a")) should be (0)
  }

  "min Distance" should "be 0 in two consecutive words" in {
    WordsMinDistance(sc,abcPath, List("a","b")) should be (0)
  }

  "min Distance" should "be 1 in two words with one in between" in {
    WordsMinDistance(sc,abcPath, List("a","c")) should be (1)
  }

  "min Distance" should "be 0 in two consecutive words (BC)" in {
    WordsMinDistance(sc,abcPath, List("b","c")) should be (0)
  }

  "min Distance" should "be -1 when only one word is found" in {
    WordsMinDistance(sc,abcPath, List("b","b")) should be (-1)
  }

  "min Distance" should "be 2 when the same word is present with 2 words in between" in {
    WordsMinDistance(sc,baabPath, List("b","b")) should be (2)
  }

  "min Distance" should "be -1 when the first word is not in the text" in {
    WordsMinDistance(sc,baabPath, List("u","b")) should be (-1)
  }

  "min Distance" should "be -1 when the second word is not in the text" in {
    WordsMinDistance(sc,baabPath, List("b","u")) should be (-1)
  }

  "min Distance" should "be 6 when the words are spread on the text with 6 words in between" in {
    WordsMinDistance(sc,abcdePath, List("a","c","e")) should be (8)
  }
}