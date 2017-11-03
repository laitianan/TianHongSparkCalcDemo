import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel, RegexTokenizer, StopWordsRemover}
import org.apache.spark.ml.linalg.{Vector => MLVector}
import org.apache.spark.mllib.clustering.{DistributedLDAModel, EMLDAOptimizer, LDA, OnlineLDAOptimizer}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.storage.StorageLevel

object LDATest {
  private case class Params(
                             input: String,
                             k: Int = 9,
                             maxIterations: Int = 10,
                             docConcentration: Double = -1,
                             topicConcentration: Double = -1,
                             vocabSize: Int = 10000,
                             stopwordFile: String = "",
                             algorithm: String = "em",
                             checkpointDir: Option[String] = None,
                             checkpointInterval: Int = 10)

  def main(args: Array[String]) {
    var params: Params = null
    if (!(System.getProperty("os.name").toLowerCase.contains("linux"))) {
      //本地
      params = Params("hdfs://192.168.10.82:9000/BybCloud/LDAdocumentMin.txt")
    } else {
      //集群
      params = Params("hdfs://192.168.10.82:9000/BybCloud/LDAdocument.txt")
    }
    run(params)
  }

  private def run(params: Params): Unit = {
    val conf = new SparkConf()
      .setAppName("LDAExample")

    if (!(System.getProperty("os.name").toLowerCase.contains("linux"))) {
      conf.setMaster("local")
    }
    //
    //    val sc = new SparkContext(conf)
    Logger.getRootLogger.setLevel(Level.WARN)

    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate()
    val sc = spark.sparkContext
    // Load documents, and prepare them for LDA.
    val preprocessStart = System.nanoTime()
    val (corpus, vocabArray, actualNumTokens) = preprocess(spark, params.input, params.vocabSize, params.stopwordFile)
    corpus.persist(StorageLevel.MEMORY_AND_DISK_SER)
    val actualCorpusSize = corpus.count()
    val actualVocabSize = vocabArray.length
    val preprocessElapsed = (System.nanoTime() - preprocessStart) / 1e9

    println()
    println(s"Corpus summary:")
    println(s"\t Training set size: $actualCorpusSize documents")
    println(s"\t Vocabulary size: $actualVocabSize terms")
    println(s"\t Training set size: $actualNumTokens tokens")
    println(s"\t Preprocessing time: $preprocessElapsed sec")
    println()

    // Run LDA.
    val lda = new LDA()

    val optimizer = params.algorithm.toLowerCase match {
      case "em" => new EMLDAOptimizer
      // add (1.0 / actualCorpusSize) to MiniBatchFraction be more robust on tiny datasets.
      case "online" => new OnlineLDAOptimizer().setMiniBatchFraction(0.05 + 1.0 / actualCorpusSize)
      case _ => throw new IllegalArgumentException(
        s"Only em, online are supported but got ${params.algorithm}.")
    }

    lda.setOptimizer(optimizer)
      .setK(params.k)
      .setMaxIterations(params.maxIterations)
      .setDocConcentration(params.docConcentration)
      .setTopicConcentration(params.topicConcentration)
      .setCheckpointInterval(params.checkpointInterval)
    if (params.checkpointDir.nonEmpty) {
      sc.setCheckpointDir(params.checkpointDir.get)
    }
    val startTime = System.nanoTime()
    val ldaModel = lda.run(corpus)
    val elapsed = (System.nanoTime() - startTime) / 1e9

    println(s"Finished training LDA model.  Summary:")
    println(s"\t Training time: $elapsed sec")

    if (ldaModel.isInstanceOf[DistributedLDAModel]) {
      val distLDAModel = ldaModel.asInstanceOf[DistributedLDAModel]
      val avgLogLikelihood = distLDAModel.logLikelihood / actualCorpusSize.toDouble
      println(s"\t Training data average log likelihood: $avgLogLikelihood")
      println()
    }

    // Print the topics, showing the top-weighted terms for each topic.
    val topicIndices = ldaModel.describeTopics(maxTermsPerTopic = 1000)
    val topics = topicIndices.map { case (terms, termWeights) =>
      terms.zip(termWeights).map { case (term, weight) => (vocabArray(term.toInt), weight) }
    }

    var set = scala.collection.mutable.Set.empty[String]


    println(s"${params.k} topics:")
    topics.zipWithIndex.foreach { case (topic, i) =>
      println(s"TOPIC $i")
      topic.foreach { case (term, weight) =>
        println(s"$term\t$weight")
        set = set.+(term)
      }
      println()
    }

    println("set: " + set.mkString("-"))
    println("setSize: " + set.size)
    spark.sparkContext.stop()
    spark.stop()
  }

  /**
    * Load documents, tokenize them, create vocabulary, and prepare documents as term count vectors.
    *
    * @return (corpus, vocabulary as array, total token count in corpus)
    */
  private def preprocess(
                          spark: SparkSession,
                          paths: String,
                          vocabSize: Int,
                          stopwordFile: String): (RDD[(Long, Vector)], Array[String], Long) = {
    import spark.implicits._
    val sc = spark.sparkContext
    val df = sc.textFile(paths)
      .map {
        r =>
          val ret = r.replaceAll("(\\d+\\.[ ])|([ ]((\\d+\\.)?)\\d+[ ])|([ ][a-zA-Z0-9]{1,2}[ ])", "")
          ret
      }.toDF("docs").persist(StorageLevel.MEMORY_AND_DISK_SER)
    // df.collect().foreach(r => println(r + "----------------------------------"))
    //",", ".", ":", ";", "?", "(", ")", "[", "]", "!", "@", "#", "%", "$", "*","~"
    val customizedStopWords: Array[String] = Array("<", "'s", "—", "c", "–", "/", "=", "+", ">", "-", ",", ".", ":", ";", "?", "(", ")", "[", "]", "!", "@", "#", "%", "$", "*", "~")
    //      if (stopwordFile.isEmpty) {
    //      Array.empty[String]
    //    } else {
    //      val stopWordText = sc.textFile(stopwordFile).collect()
    //      stopWordText.flatMap(_.stripMargin.split("\\s+"))
    //    }
    val tokenizer = new RegexTokenizer()
      .setInputCol("docs")
      .setOutputCol("rawTokens")
    val stopWordsRemover = new StopWordsRemover()
      .setInputCol("rawTokens")
      .setOutputCol("tokens")
    stopWordsRemover.setStopWords(stopWordsRemover.getStopWords ++ customizedStopWords)
    val countVectorizer = new CountVectorizer()
      .setVocabSize(vocabSize)
      .setInputCol("tokens")
      .setOutputCol("features")

    val pipeline = new Pipeline()
      .setStages(Array(tokenizer, stopWordsRemover, countVectorizer))

    val model = pipeline.fit(df)
    val documents = model.transform(df)
      .select("features")
      .rdd
      .map { case Row(features: MLVector) => Vectors.fromML(features) }
      .zipWithIndex()
      .map(_.swap)
    documents.collect().foreach(println)
    (documents, model.stages(2).asInstanceOf[CountVectorizerModel].vocabulary, // vocabulary
      documents.map(_._2.numActives).sum().toLong) // total token count
  }
}

