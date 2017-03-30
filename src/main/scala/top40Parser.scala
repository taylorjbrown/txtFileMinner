 import scala.util.parsing.combinator._

class p5Parser extends RegexParsers{
	def trash: Parser[String] = """([^']*)([^\\D])(,)""".r ^^ {_.toString}
	def artist: Parser[String] ="""(')([^']*)(')""".r   ^^ {_.toString}
	def song: Parser[String] = """(')([^']*)(')""".r  ^^ {_.toString} 
	def numOneFlag: Parser[Int] = """[^\\D]""".r ^^ {_.toInt}
	def fullTup: Parser[Tuple3[String,String,Int]] = trash ~ artist~""",""" ~ song ~ ""","""~ numOneFlag ^^ { case trash ~ artist~""",""" ~ song ~ ""","""~ numOneFlag=> 
	(artist.substring(artist.indexOf("'")+1,artist.length-1),song.substring(song.indexOf("'")+1,song.length-1),numOneFlag)}
}