package bigdata.app;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.time.Instant;
import java.util.Comparator;
import java.util.Date;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import bigdata.objects.Asset;
import bigdata.objects.AssetFeatures;
import bigdata.objects.AssetMetadata;
import bigdata.objects.AssetRanking;
import bigdata.objects.StockPrice;

import bigdata.transformations.filters.DateFilter;
import bigdata.transformations.filters.NullPriceFilter;
import bigdata.transformations.maps.IndicatorCalculationMap;
import bigdata.transformations.maps.PriceReaderMap;
import bigdata.transformations.maps.PricePairingMap;
import bigdata.transformations.pairing.AssetMetadataPairing;
import scala.Tuple2;

public class AssessedExercise {

    public static void main(String[] args) throws InterruptedException {
        //--------------------------------------------------------
        // Static Configuration
        //--------------------------------------------------------
        String datasetEndDate = "2020-04-01";
        double volatilityCeiling = 4;
        double peRatioThreshold = 25;
    
        long startTime = System.currentTimeMillis();
        
        // The code submitted for the assessed exerise may be run in either local or remote modes
        // Configuration of this will be performed based on an environment variable
        String sparkMasterDef = System.getenv("SPARK_MASTER");
        if (sparkMasterDef==null) {
            File hadoopDIR = new File("resources/hadoop/"); // represent the hadoop directory as a Java file so we can get an absolute path for it
            System.setProperty("hadoop.home.dir", hadoopDIR.getAbsolutePath()); // set the JVM system property so that Spark finds it
            sparkMasterDef = "local[4]"; // default is local mode with two executors
        }
        
        String sparkSessionName = "BigDataAE"; // give the session a name
        
        // Create the Spark Configuration 
        SparkConf conf = new SparkConf()
                .setMaster(sparkMasterDef)
                .setAppName(sparkSessionName);

        // Create the spark session
        SparkSession spark = SparkSession
                  .builder()
                  .config(conf)
                  .getOrCreate();
    
        
        // Get the location of the asset pricing data
        String pricesFile = System.getenv("BIGDATA_PRICES");
        if (pricesFile==null) pricesFile = "resources/all_prices-noHead.csv"; // default is a sample with 3 queries
        
        // Get the asset metadata
        String assetsFile = System.getenv("BIGDATA_ASSETS");
        if (assetsFile==null) assetsFile = "resources/stock_data.json"; // default is a sample with 3 queries
        
        
        //----------------------------------------
        // Pre-provided code for loading the data 
        //----------------------------------------
        
        // Create Datasets based on the input files
        
        // Load in the assets, this is a relatively small file
        // Use the absolute path of the Java underlying classes to forcefully bypass Python data source scanning
        Dataset<Row> assetRows = spark.read()
            .format("org.apache.spark.sql.execution.datasources.json.JsonFileFormat")
            .option("multiLine", true)
            .load(assetsFile);      
            
        //assetRows.printSchema();
        //System.err.println(assetRows.first().toString());
        JavaPairRDD<String, AssetMetadata> assetMetadata = assetRows.toJavaRDD().mapToPair(new AssetMetadataPairing());
        
        // Load in the prices, this is a large file (not so much in data size, but in number of records)
        // Similarly, use the absolute path of the underlying CSV class to bypass Python scanning
        Dataset<Row> priceRows = spark.read()
            .format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
            .load(pricesFile);      
            
        Dataset<Row> priceRowsNoNull = priceRows.filter(new NullPriceFilter()); // filter out rows with null prices
        Dataset<StockPrice> prices = priceRowsNoNull.map(new PriceReaderMap(), Encoders.bean(StockPrice.class)); // Convert to Stock Price Objects
        
    
        AssetRanking finalRanking = rankInvestments(spark, assetMetadata, prices, datasetEndDate, volatilityCeiling, peRatioThreshold);
        
        System.out.println(finalRanking.toString());
        
        System.out.println("Holding Spark UI open for 1 minute: http://localhost:4040");
        
        Thread.sleep(60000);
        
        // Close the spark session
        spark.close();
        
        String out = System.getenv("BIGDATA_RESULTS");
        String resultsDIR = "results/";
        if (out!=null) resultsDIR = out;
        
        
        
        long endTime = System.currentTimeMillis();
        
        try {
            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File(resultsDIR).getAbsolutePath()+"/SPARK.DONE")));
            
            Instant sinstant = Instant.ofEpochSecond( startTime/1000 );
            Date sdate = Date.from( sinstant );
            
            Instant einstant = Instant.ofEpochSecond( endTime/1000 );
            Date edate = Date.from( einstant );
            
            writer.write("StartTime:"+sdate.toGMTString()+'\n');
            writer.write("EndTime:"+edate.toGMTString()+'\n');
            writer.write("Seconds: "+((endTime-startTime)/1000)+'\n');
            writer.write('\n');
            writer.write(finalRanking.toString());
            writer.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        
    }


    public static AssetRanking rankInvestments(SparkSession spark, JavaPairRDD<String, AssetMetadata> assetMetadata, Dataset<StockPrice> prices, String datasetEndDate, double volatilityCeiling, double peRatioThreshold) {
        
        // 1. Data Cleaning: Apply DateFilter at the Dataset level
        JavaRDD<StockPrice> filteredPrices = prices
                .filter(new DateFilter(datasetEndDate))
                .javaRDD(); 

        // 2. Core Matching: Extract the stock ticker using getStockTicker()
        JavaPairRDD<String, Iterable<StockPrice>> groupedPrices = filteredPrices
        .mapToPair(new PricePairingMap()) 
        .groupByKey();

        // 3. Calculate Indicators: Call the core calculation logic
        JavaPairRDD<String, Tuple2<Double, Double>> indicators = groupedPrices
                .mapToPair(new IndicatorCalculationMap());

        // 4. Combined Filtering: Filter high volatility -> Join metadata -> Filter non-compliant P/E ratios
        JavaPairRDD<String, Tuple2<Tuple2<Double, Double>, AssetMetadata>> finalCandidates = indicators
                .filter(t -> t._2._1 < volatilityCeiling)
                .join(assetMetadata)
                .filter(t -> t._2._2.getPriceEarningRatio() > 0 && t._2._2.getPriceEarningRatio() < peRatioThreshold);

        // 5. Sorting: Directly use Lambda to extract the Top 5 based on returns
        List<Tuple2<String, Tuple2<Tuple2<Double, Double>, AssetMetadata>>> top5 = 
            finalCandidates.takeOrdered(5, (Serializable & Comparator<Tuple2<String, Tuple2<Tuple2<Double, Double>, AssetMetadata>>>) 
            (o1, o2) -> Double.compare(o2._2._1._2, o1._2._1._2));

        // 6. Encapsulate Results: Assemble exactly according to the provided Asset and AssetFeatures structure
        AssetRanking finalRanking = new AssetRanking();
        
        Asset[] assets = new Asset[5]; 
        
        for (int i = 0; i < top5.size() && i < 5; i++) {
			/**
			 * entry (Tuple2)
 			├── _1 : String (Ticker， "AAPL")
			└── _2 : Tuple2 
				├── _1 : Tuple2 
				│    ├── _1 : Double
				│    └── _2 : Double
				└── _2 : AssetMetadata 
			 */
            var entry = top5.get(i);
            String ticker = entry._1;
            double vol = entry._2._1._1;
            double ret = entry._2._1._2;
            AssetMetadata meta = entry._2._2;

            AssetFeatures features = new AssetFeatures();
            features.setAssetReturn(ret);
            features.setAssetVolitility(vol);
            features.setPeRatio(meta.getPriceEarningRatio());

            Asset a = new Asset();
            a.setTicker(ticker);
            a.setName(meta.getName());
            a.setIndustry(meta.getIndustry());
            a.setSector(meta.getSector());
            a.setFeatures(features);

            // Store in the array
            assets[i] = a;
        }

        // Put the assembled array into the Ranking object
        finalRanking.setAssetRanking(assets);
        
        return finalRanking;
    }
}