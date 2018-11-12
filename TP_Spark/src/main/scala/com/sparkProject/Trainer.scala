package com.sparkProject

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.ml.feature.{RegexTokenizer,CountVectorizer,IDF, StringIndexer,VectorAssembler,OneHotEncoderEstimator}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit}

object Trainer {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAll(Map(
      "spark.scheduler.mode" -> "FIFO",
      "spark.speculation" -> "false",
      "spark.reducer.maxSizeInFlight" -> "48m",
      "spark.serializer" -> "org.apache.spark.serializer.KryoSerializer",
      "spark.kryoserializer.buffer.max" -> "1g",
      "spark.shuffle.file.buffer" -> "32k",
      "spark.default.parallelism" -> "12",
      "spark.sql.shuffle.partitions" -> "12",
      "spark.driver.maxResultSize" -> "2g"
    ))

    val spark = SparkSession
      .builder
      .config(conf)
      .appName("TP_spark")
      .getOrCreate()


    /*******************************************************************************
      *
      *       TP 3
      *
      *       - lire le fichier sauvegarder précédemment
      *       - construire les Stages du pipeline, puis les assembler
      *       - trouver les meilleurs hyperparamètres pour l'entraînement du pipeline avec une grid-search
      *       - Sauvegarder le pipeline entraîné
      *
      *       if problems with unimported modules => sbt plugins update
      *
      ********************************************************************************/


    // On charge les données des fichiers parquet dans un dataframe
    val df = spark.read.option("header",true).parquet("/Users/stephanetram/Documents/MSBGD/INF729 - Hadoop-Spark/Spark/TP/prepared_trainingset")


    /* on va définir toutes les transformations du dataframe :
      - le regex tokenizer va transfromer le texte en tokens dans une colonne tokens
      - le stop words remover va retirer une liste d'occurences non significative
      - count vectorizer va transformer les données en vecteur
      - on index ensuite les colonnes country et currency
      - on encode ensuite ces colonnes en one hot
      - puis on applique un assmebler qui va assembler toutes nos features dans une colonne de features
     */

    val tokenizer= new RegexTokenizer().setPattern("\\W+").setGaps(true).setInputCol("text").setOutputCol("tokens")
    val remover = new StopWordsRemover().setInputCol("tokens").setOutputCol("filtered")
    val counter = new CountVectorizer().setInputCol("filtered").setOutputCol("vectored")
    val idfModel = new IDF().setInputCol("vectored").setOutputCol("tfidf")
    val countryIndexer = new StringIndexer().setInputCol("country2").setOutputCol("countryIndex").setHandleInvalid("skip")
    val ccyIndexer = new StringIndexer().setInputCol("currency2").setOutputCol("currencyIndex").setHandleInvalid("skip")
    val encoder = new OneHotEncoderEstimator().setInputCols(Array("countryIndex","currencyIndex")).setOutputCols(Array("country_onehot","currency_onehot"))
    val assembler = new VectorAssembler().setInputCols(Array("tfidf", "days_campaign", "hours_prepa", "goal", "country_onehot", "currency_onehot")).setOutputCol("features")

    /*
    On définit un modèle de régression logistique à appliquer
     */

    val lr = new LogisticRegression()
      .setElasticNetParam(0.0)
      .setFitIntercept(true)
      .setFeaturesCol("features")
      .setLabelCol("final_status")
      .setStandardization(true)
      .setPredictionCol("predictions")
      .setRawPredictionCol("raw_predictions")
      .setThresholds(Array(0.7, 0.3))
      .setTol(1.0e-6)
      .setMaxIter(300)

    /*
    On split le dataframe df en un jeu d'entrainement et un jeu de test
     */

    val Array(training, test) = df.randomSplit(Array(0.9, 0.1), seed = 100)

    /*
    Le pipeline va nous permettre d'appliquer toutes les transformations à la suite sur le dataframe training
    puis d'entrainer le modèle de regression sur ce jeu
    enfin on applique le modèle entrainé sur notre jeu de test et on affiche les résultats
     */

    val pipeline = new Pipeline().setStages(Array(tokenizer,remover,counter,idfModel,countryIndexer,ccyIndexer,encoder,assembler,lr))
    val model =pipeline.fit(training)
    val dfRes = model.transform(test)
    println(dfRes.show())


    //on crée un évaluateur de métrique f1

    val f1Score = new MulticlassClassificationEvaluator().setMetricName("f1").setLabelCol("final_status").setPredictionCol("predictions")

    //on crée une grille de paramètres pour entrainer notre modèle

    val paramGrid = new ParamGridBuilder()
      .addGrid(lr.regParam, Array(10e-8, 10e-6))
      .addGrid(counter.minDF, Array(55.0,75.0))
      .build()

    //on crée un objet de cross validation

    val trainValidationSplit = new TrainValidationSplit()
      .setEstimator(lr)
      .setEvaluator(f1Score)
      .setEstimatorParamMaps(paramGrid)
      .setTrainRatio(0.7)

    /*
    Comme pour le modèle simple on construit un pipeline pour appliquer successivement les transformations puis
    entrainer les modèles puis appliquer les modèles au jeu de test
    */
    val pipelineCV = new Pipeline().setStages(Array(tokenizer,remover,counter,idfModel,countryIndexer,ccyIndexer,encoder,assembler,trainValidationSplit))
    val modelCV = pipelineCV.fit(training)
    val dfCV = modelCV.transform(test).select("features", "final_status", "predictions")
    println(dfCV.show())


  }
}
