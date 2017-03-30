import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import java.lang.String._
import scala.io._
import scala.util.parsing.combinator._
import scala.collection._

object sparkTop40Search extends p5Parser{
     
     def main(args: Array[String]){
      val top40Parser = new p5Parser  ()
	  val sc = new SparkContext("local[8]", "First Spark App")
      try{
      val data = sc.textFile("data/top40.txt").map(combinFinder(_))
      //
      // Where 5 a. is accomplishe:
      //
      val wordsToExclude = List("", "THE","YOU","I","A","ME","TO","OF","MY","IN","AND","ON","IT","YOUR","BE","IS","FOR","I*M")
      val songsWordOccurance = data.flatMap{ case (artist,song,numOneFlag)=> song.replaceAll("""[\p{Punct}&&[^.|*]]""","").split(" ")}
      .map{ case (word) => if(!wordsToExclude.contains(word)){word} else {(" ")}}
      .filter(!_.contains(" "))
	  .map{ wordOccurance => (wordOccurance,1)}.reduceByKey(_+_).sortBy(-_._2)
	  //
	  // Where 5 a. is printed out, 
	  // feel free to increase the amount of songs printed:
	  //
    songsWordOccurance.foreach(println)
         val topWords15 = songsWordOccurance.take(15)


       	val artistCountTop40 = data.map{ case (artist,song,numOneFlag)=> (artist,1)}.reduceByKey(_+_).sortBy(-_._2)

       	val topArtistOfTop40 = artistCountTop40.take(15)

        val artistCountAt1Spot = data.filter(_._3 == 1).map{ case (artist,song,numOneFlag) => (artist,1)}.reduceByKey(_+_).sortBy(-_._2)
        
        val artistWithMost1 = artistCountAt1Spot.take(15)
     

       
       
		//probablityOfFreqWord.foreach(println)


	  
        //new

        val num1 = data.filter(_._3 == 1).map{case (artist, song, numOneFlag) => (song,1)}.reduceByKey(_+_).sortBy(-_._2).count.toDouble
         
       val probability1 = for{x<-topWords15 
        	val word = x._1
        	val count = x._2
        	
        	val songsAndWords = data.map{case (artist, song, cont) => (song.replaceAll(",", "").trim,cont)}
			.map{case (song, cont) => (song.replaceAll("""[\p{Punct}&&[^.|*]]""",""),cont)}
			.filter(_._1.split(" ").contains(word)).count()
			
			val num1SongandWord = data.map{case (artist, song, cont) => (song.replaceAll(",", "").trim,cont)}
			.map{case (song, cont) =>if(cont == 1){(song.replaceAll("""[\p{Punct}&&[^.|*]]""",""),cont)} else{(" ",0)}}
			.filter(_._1.split(" ").contains(word)).count()
			
			val tup = (word,((num1SongandWord/songsAndWords.toDouble).toDouble*100))} yield tup

		val probability2 = for{x<-topWords15 
        	val word = x._1
        	val count = x._2

			val num1SongandWord = data.map{case (artist, song, cont) => (song.replaceAll(",", "").trim,cont)}
			.map{case (song, cont) =>if(cont == 1){(song.replaceAll("""[\p{Punct}&&[^.|*]]""",""),cont)} else{(" ",0)}}
			.filter(_._1.split(" ").contains(word)).count()
			
			val tup = (word,((num1SongandWord/num1).toDouble*100))} yield tup

		/*justMapWordsSong.foreach(println)										
		justMapNum1SongWord.foreach(println)

		val combine = justMapNum1SongWord.merged(justMapWordsSong)({case ((word,count1),(_,count2)) => (word, (count1/count2))})
		combine.foreach(println)*/
		
       //val probabilityOf1 = justMap.map{ case(word,count) => (word, (/count.toDouble)) }
       //println(songsContainLove.toDouble/num1.count.toDouble)

       //val countOfLove = justMap.map{ case("LOVE",count) => count }.first()

       val songFreqWords = data.flatMap{ case (artist,song,numOneFlag)=> song.replaceAll("""[\p{Punct}&&[^.|*]]""","").split(" ")}
       .filter(!_.contains(" "))
	   .map{ wordOccurance => (wordOccurance,1)}.reduceByKey(_+_).sortBy(-_._2)

	   val totalWordsCount = data.flatMap{ case (artist,song,numOneFlag)=> song.replaceAll("""[\p{Punct}&&[^.|*]]""","").split(" ")}
	   .map{ wordOccurance => (wordOccurance,1)}.reduceByKey(_+_).sortBy(-_._2).count()

	   val probablityOfFreqWord = songFreqWords.map{ case (word,count) => (word, ((count.toDouble)/(totalWordsCount.toDouble)*100))}.take(15)

        val songAt1Spot = data.filter(_._3 == 1).map{ case (artist,song,numOneFlag) => (song,1)}.reduceByKey(_+_).sortBy(-_._2)
       
       val totalSongs = data.map{ case (artist,song,numOneFlag)=> (song,1)}.reduceByKey(_+_).sortBy(-_._2).count()

        val probablityOfSongTop = songAt1Spot.map{ case (song,count) => (song, ((count.toDouble)/(totalSongs.toDouble).toDouble*100))}.take(15)
    
       println("-----------------------------------------------")
       println("Top 15 Word Counts: ")
       println("___________________")
            topWords15.foreach(println)
       println("-----------------------------------------------")
       println("Top 15 Artist With The Most Songs In Top 40 List: ")
       println("________________________________________________")
            topArtistOfTop40.foreach(println)
       println("-----------------------------------------------")
       println("Top 15 Artist With The Most Songs At Number 1 Spot: ")
       println("_______________________________________________")
            artistWithMost1.foreach(println)
       println("-----------------------------------------------")
       println("Top 15 Word Prob. In % Of((Song With This Word And Are Also # 1)/((All Aongs With Frequent Word)) ")
	   println("_____________________________________________________________________________________________")
	        probability1.foreach(println)
       println("-----------------------------------------------")
       println("Top 15 Word Prob. In % Of((Song With This Word And Are Also # 1)/((# Of # 1 Songs)) ")
	   println("____________________________________________________________________________")
	   	    probability2.foreach(println)
       println("-----------------------------------------------")
       println(" ")
       println(" ")
       println(" ")
       println("NOT IN ASSIGNMENT, BUT FOUND:")
       println(" ")
       println(" ")
       println("-----------------------------------------------")
       println("Top 15 Most Probable Frequent Words In % Of Top 40 Song Titles: ")
       println("_________________________________________________")
            probablityOfFreqWord.foreach(println)
       println("-----------------------------------------------")
       println("Top 15 Highest Probaility Of A Song Being Top In %: ")
       println("________________________________________________")
       		probablityOfSongTop.foreach(println)
       println("-----------------------------------------------")

       
        
		//probablityOfSongTop.foreach(println)
         //songAt1Spot.foreach(println)
	     //println(totalSongs)*

         }catch{ case e: Exception => sc.stop()}
            sc.stop()
     }

     def combinFinder(keyWords:String): Tuple3[String,String,Int] = {
    	parse(fullTup, keyWords) match{
    	case Success  (matched,_) => matched
        case Failure (msg,_) => ("Failure: ", msg ,-1)
        case Error (msg,_) => ("Error: ", msg, -2)
      		}
    }
}
      //val excludeWord = values.map{ case (word) => if(!wordsToExclude.contains(word)){word}}
      //val noEmpties =excludeWord.collect(_.isEmpty)
      	//.filterNot(_.equals((" ",1)))
      //.reduceByKey(_+_).sortBy(-_._2)
      //val excludeWord = word.map{ case (word)=> if (!wordsToExclude.contains(word)){(word,1)}} 
      /*val songWords = for{ song <- values
                          words = song.replaceAll("""[\p{Punct}&&[^.|*]]""","").trim.split(" ").filterNot(wordsToExclude.contains(_))}yield words.reduce*/
      //val mapCounts = values.flatMap(song => (song ,1))
      //val wordCounts = songWords.groupBy(identity).mapValues(_.size)
      /* = for{ song<- songWords
          counts = song.groupBy(identity).map{ song_.size)}yield counts*/

        /*def filterLine(line:String):String={   
          val lineFinalIndex = line.length()-1
          val removingIndex = line.indexOfSlice(", '")+1 
          val tokens = line.substring(removingIndex,lineFinalIndex).replaceAll("""[\p{Punct}&&[^.|,|*|'||\|/|-]]""", "")
          tokens
     }*/
     /*trait Flatten[M] extends (M => Seq[(String,String,Int)]){
     	def apply(m : M) : Seq[(String,String,Int)]
     }

	 implicit def flattenParseResult[T]
	  (implicit flattenT : Flatten[T]) = new Flatten[ParseResult[T]]{
	 	def apply(p: ParseResult[T]) = (p map flattenT) getorElse Nil
	 }     
	 def flatten[P](p : P)(implicit flatten : Flatten[P]) = flatten(p)
     
     def flatten(res:Any): Seq[(String,String,Int)] = res match{
     	case x:(String,String,Int) => Seq[_]
     }*/  
		
      


       /*val keyWords = for{line <- Source.fromFile("top40.txt").getLines
                              words = filterLine(line)
                              finalParse = top40Parser.fullTup(new CharSequenceReader (words))} yield finalParse*/
       //keyWords.foreach(println)
       //val top40DataSet = sc.parallelize(keyWords)
     
    /*def mostFreq (data: RDD[Tuple3[String,String,Int]]): hashMap(String,Int){
          val wordsToExclude = List("THE","YOU","I","A","ME","TO","OF","MY","IN","AND","ON","IT","YOUR","BE","IS","FOR","I*M")
    } */    

    // P() = C(num1 n word) / c(num1)
    // P() = c(num1 n word) / c(word)




//probablity given the word twilight will be 100% since there is only 
//one song that contains the word twilight and it was a number 1 hit
