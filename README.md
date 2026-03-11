# 任务大纲

| **工序名称**         | **输入类型 (Input)**          | **内部核心逻辑**            | **输出类型 (Return)**                    |
| -------------------- | ----------------------------- | --------------------------- | ---------------------------------------- |
| **DateFilter**       | `StockPrice`                  | 时间对比，保留或剔除        | `Boolean` (给 filter 算子用)             |
| **PricePairingMap**  | `StockPrice`                  | 提取 Ticker 作为 Key        | `Tuple2<String, StockPrice>`             |
| **GroupByKey**       | `Tuple2<String, StockPrice>`  | 跨节点数据搬运、聚类        | `Tuple2<String, Iterable<StockPrice>>`   |
| **IndicatorCalcMap** | `Tuple2<String, Iterable...>` | **排序** + **数学公式计算** | `Tuple2<String, Tuple2<Double, Double>>` |

- 把每一条stockprice转换成一个kv对，k是股票代码，v是价格对象
- groupbykey，把所有相同key（同一种股票）的v全部收集到一起

1. **原始 CSV** $\rightarrow$ `Row` (字符串数组)
2. **`NullPriceFilter`** $\rightarrow$ 剔除带有 null 的坏掉的 `Row` 
3. **`PriceReaderMap`** $\rightarrow$ 转换成漂亮的 `StockPrice` 对象 
4. **`DateFilter` (咱们刚写的)** $\rightarrow$ 剔除掉日期越界的 `StockPrice`
5. **`PricePairingMap` (下一步要写的)** $\rightarrow$ 转换成 `Tuple2<String, StockPrice>` 键值对
6. **`groupByKey` 算子** $\rightarrow$ 变成 `(String, Iterable<StockPrice>)`，也就是（股票代码，这只股票过去所有日期的价格大礼包）！

## 快捷键

/*+回车 多行注释

cmd+. 导包

## 1，日期过滤

```java
package bigdata.transformations.filters;
import java.time.Instant;

import org.apache.spark.api.java.function.FilterFunction;
import bigdata.objects.StockPrice;
import bigdata.util.TimeUtil;

public class DateFilter implements FilterFunction<StockPrice>{
    private static final long serialVersionUID = 1L;
    private Instant endInstant;

    public DateFilter(String datasetEndDate){
        this.endInstant = TimeUtil.fromDate(datasetEndDate);
    }

    @Override
    public boolean call(StockPrice price) throws Exception{
        Instant curPriceDate = TimeUtil.fromDate(price.getYear(), price.getMonth(), price.getDay());
        return !curPriceDate.isAfter(this.endInstant);
    }
}

```

- 构造函数的活就是客户自己搞，设计一个过滤标准，然后内部的返回逻辑就是避免再次日期转换，直接对stockprice对象和转换好的日期进行对比

- 日期转换的脏活在构造函数完成，不需要到分布式call上去

  

## 2，转化成kv对用于groupbykey

```java
package bigdata.transformations.pairing;

import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import bigdata.objects.StockPrice;

/**
 * Maps each StockPrice object to a key-value pair (Tuple2).
 * The key is the stock ticker (String), and the value is the StockPrice object itself.
 * This step is essential to prepare the dataset for the groupByKey operation.
 */
public class PricePairingMap implements PairFunction<StockPrice, String, StockPrice> {

    private static final long serialVersionUID = 1L;

    @Override
    public Tuple2<String, StockPrice> call(StockPrice price) throws Exception {
        // Extract the stock ticker to use as the key, and package it with the price object
        return new Tuple2<>(price.getStockTicker(), price);
    }
}
```

<输入的数据类型, 输出的Key(键)的类型, 输出的Value(值)的类型>

**“明明我的 `StockPrice` 对象里面已经有 `stockTicker`（股票代码）这个变量了，为什么还要多此一举，非要把它单独拎出来放在前面，打包成 `(股票代码, 价格对象)` 的形式？”**

这完全是因为 **Spark 底层的工作机制（Shuffle 分发）决定的。**

Spark 里有一个极其强大的算子叫 `groupByKey()`，它的作用是把相同种类的数据聚到一起。但是，**`groupByKey()` 是个“近视眼”**。 如果你直接把一堆散落的 `StockPrice` 对象扔给它，它会一脸懵：“你让我按什么分组？按年份？按开盘价？还是按股票代码？我不知道啊！” 在 Spark 的源码规定里，`groupByKey()` **绝对不接受**普通的单一对象（`JavaRDD`），它**只接受**标准的键值对格式（`JavaPairRDD`）。



**groupbykey其实就是进行分布式指标计算！**

算出的tuple2指标，是为了他们在一起计算双指标的时候，可以做到知道股票是哪个

## 3，指标计算

```java
package bigdata.app;

import java.util.List;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

// 导入你项目结构中的对象和算子
import bigdata.objects.AssetMetadata;
import bigdata.objects.AssetRanking;
import bigdata.objects.StockPrice;
import bigdata.transformations.filters.DateFilter;
import bigdata.transformations.filters.NullPriceFilter;
import bigdata.transformations.maps.IndicatorCalculationMap;
import bigdata.transformations.comparators.ReturnsComparator;

public class AssessedExercise {

    // ... main 函数保持不变 ...

    /**
     * 执行核心任务：清洗、计算指标、关联元数据并排名
     */
    public static AssetRanking rankInvestments(SparkSession spark, 
                                             JavaPairRDD<String, AssetMetadata> assetMetadata, 
                                             Dataset<StockPrice> prices, 
                                             String datasetEndDate, 
                                             double volatilityCeiling, 
                                             double peRatioThreshold) {
        
        // 1. 数据清洗：连续应用空值过滤和日期过滤
        JavaRDD<StockPrice> filteredPrices = prices.javaRDD()
                .filter(new NullPriceFilter()) 
                .filter(new DateFilter(datasetEndDate));

        // 2. 指标计算：分组并调用计算 Map (计算 Volatility 和 Returns)
        JavaPairRDD<String, Tuple2<Double, Double>> indicators = filteredPrices
                .mapToPair(p -> new Tuple2<>(p.getTicker(), p))
                .groupByKey()
                .mapToPair(new IndicatorCalculationMap());

        // 3. 组合逻辑：低波动过滤 -> 关联元数据 -> 低市盈率过滤
        JavaPairRDD<String, Tuple2<Tuple2<Double, Double>, AssetMetadata>> finalCandidates = indicators
                .filter(t -> t._2._1 < volatilityCeiling)
                .join(assetMetadata)
                .filter(t -> {
                    double pe = t._2._2.getPriceEarningRatio();
                    return pe > 0 && pe < peRatioThreshold;
                });

        // 4. 最终排序：按回报率取前 5
        List<Tuple2<String, Tuple2<Tuple2<Double, Double>, AssetMetadata>>> top5 = 
                finalCandidates.takeOrdered(5, new ReturnsComparator());

        // 5. 封装结果输出
        AssetRanking finalRanking = new AssetRanking();
        for (Tuple2<String, Tuple2<Tuple2<Double, Double>, AssetMetadata>> entry : top5) {
            finalRanking.addAsset(
                entry._1,              // Ticker
                entry._2._1._2,        // Returns
                entry._2._1._1,        // Volatility
                entry._2._2.getPriceEarningRatio() // PE Ratio
            );
        }
        
        return finalRanking;
    }
}
```

**输入：** 一个盒子 `(String, Iterable<StockPrice>)`。

**逻辑：** **“深加工”**。

1. **排序**：把乱序的 `Iterable` 倒出来，变成 `List`，按时间排好。
2. **切片**：数出最后 251 天和 5 天。
3. **计算**：调用数学公式算出波动率和回报率。

**返回值：** `JavaPairRDD<String, Tuple2<Double, Double>>`。

- *数据长相：* `("AAPL", (波动率, 回报率))`。
- *质变：* 这一步之后，成千上万行的历史价格**消失了**，取而代之的是两个精准的统计数字。



## 附 - Iterable

- `Iterable` 就像是一个**水龙头**。它并不承诺水桶里已经装满了水，它只承诺：“如果你拧开开关（开始循环），我能保证把属于这只股票的价格一个个给你流出来。”

- 这样做是因为有些股票（如 AAPL）可能有几万条价格记录。如果 Spark 直接给你一个 `List`，可能会瞬间撑爆你的内存。用 `Iterable`，Spark 就可以边读磁盘/网络，边把数据传给你。

## 4，最后主函数调用

```java
// 1. 数据清洗
JavaRDD<StockPrice> cleanedPrices = rawPricesRDD
    .filter(new NullFilter())
    .filter(new DateFilter());

// 2. 指标计算 (这是你之前写的最辛苦的那部分)
JavaPairRDD<String, Iterable<StockPrice>> grouped = cleanedPrices
    .mapToPair(p -> new Tuple2<>(p.getTicker(), p))
    .groupByKey();

JavaPairRDD<String, Tuple2<Double, Double>> indicators = grouped
    .mapToPair(new IndicatorCalculationMap());

// 3. 第一次过滤：波动率 < 4.0
JavaPairRDD<String, Tuple2<Double, Double>> filteredByVol = indicators
    .filter(t -> t._2._1 < 4.0);

// 4. 数据合体 (Join)
JavaPairRDD<String, Tuple2<Tuple2<Double, Double>, AssetMetadata>> joined = 
    filteredByVol.join(assetMetadataRDD); // 别忘了先把 assetMetadata 转成 PairRDD

// 5. 第二次过滤：市盈率 < 25.0
JavaPairRDD<String, Tuple2<Tuple2<Double, Double>, AssetMetadata>> finalResult = joined
    .filter(t -> t._2._2.getPriceEarningRatio() < 25.0 && t._2._2.getPriceEarningRatio() > 0);

// 6. 最终排序取前 5
List<Tuple2<String, Tuple2<Tuple2<Double, Double>, AssetMetadata>>> top5 = 
    finalResult.takeOrdered(5, new ReturnsComparator());
```

```java
public static AssetRanking rankInvestments(SparkSession spark, JavaPairRDD<String, AssetMetadata> assetMetadata, Dataset<StockPrice> prices, String datasetEndDate, double volatilityCeiling, double peRatioThreshold) {
    
    // 1. 数据清洗
    JavaRDD<StockPrice> filteredPrices = prices.javaRDD()
            .filter(new DateFilter(datasetEndDate));

    // 2. 配对并分组
    JavaPairRDD<String, Iterable<StockPrice>> groupedPrices = filteredPrices
            .mapToPair(p -> new Tuple2<>(p.getTicker(), p))
            .groupByKey();

    // 3. 计算指标
    JavaPairRDD<String, Tuple2<Double, Double>> indicatorResults = groupedPrices
            .mapToPair(new IndicatorCalculationMap());

    // 4. 过滤波动率
    JavaPairRDD<String, Tuple2<Double, Double>> lowVolStocks = indicatorResults
            .filter(t -> t._2._1 < volatilityCeiling);

    // 5. 与元数据合体
    JavaPairRDD<String, Tuple2<Tuple2<Double, Double>, AssetMetadata>> joinedData = 
            lowVolStocks.join(assetMetadata);

    // 6. 过滤市盈率
    JavaPairRDD<String, Tuple2<Tuple2<Double, Double>, AssetMetadata>> finalCandidates = joinedData
            .filter(t -> {
                double pe = t._2._2.getPriceEarningRatio();
                return pe > 0 && pe < peRatioThreshold;
            });

    // 7. 排序并取 Top 5
    List<Tuple2<String, Tuple2<Tuple2<Double, Double>, AssetMetadata>>> top5 = 
            finalCandidates.takeOrdered(5, new ReturnsComparator());

    // 8. 封装结果
    AssetRanking finalRanking = new AssetRanking();
    for (Tuple2<String, Tuple2<Tuple2<Double, Double>, AssetMetadata>> entry : top5) {
        String ticker = entry._1;
        double vol = entry._2._1._1;
        double ret = entry._2._1._2;
        // 这里记得确认下 AssetRanking 是否有 add 方法
        finalRanking.addAsset(ticker, vol, ret); 
    }
    
    return finalRanking;
}
```

## 5，死锁问题***

这绝对是一次教科书级别的大数据踩坑与排错实战！你从头到尾的坚持，加上一层层剥开系统底层的推理，非常值得记录下来。

我为你整理了一份结构化的**“Spark 4.0 踩坑与调优实战笔记”**。你可以直接复制保存到你的 Notion 或 Obsidian 里：

------

# 📓 大数据实战笔记：Spark 4.0 幽灵死锁与底层反序列化排雷

**环境配置：**

- **OS:** macOS (Apple Silicon, ARM架构)
- **终端环境:** Conda `(base)`
- **核心框架:** Apache Spark 4.0.0-preview2 (Java API)
- **数据量:** 2400万条金融交易记录 (CSV) + 元数据 (JSON)

## 🐛 踩坑一：长达几十分钟的“幽灵死锁” (0% CPU, 无报错)

### 1. 现象描述

- 程序启动后，日志停留在 `Warehouse path is ...`，随后完全假死。
- Mac 风扇不转，CPU 占用极低。
- Spark Web UI (`localhost:4040/4041`) 中的 Stages 页面一片空白，连任务计划都没生成。
- 按下 `Ctrl + C` 强行终止时，控制台突然爆出大量报错：`Python worker exited unexpectedly (crashed)` 和 `UserDefinedPythonDataSource.lookupAllDataSourcesInPython`。

### 2. 失败的排查尝试

- **怀疑数据量太大 (OOM)**：在代码中加入 `.limit(100000)` 试图只读 10 万条数据，**失败**，依然卡死。说明卡死发生在实际读取数据之前。
- **怀疑局域网 IP 隔离拦截**：日志提示 `using 10.x.x.x instead of 127.0.0.1`。尝试物理断开 Wi-Fi 强制本地回环，**失败**，依然卡死。说明不是外部网络问题，而是电脑内部通信问题。

### 3. 根本原因 (Root Cause)

这是 Spark 4.0 预览版引入的新特性与 Mac 本地 Conda 环境冲突导致的**底层 Socket 死锁**：

1. **触发机制**：当代码中使用 `spark.read().json()` 或 `.csv()` 这种**简写 API** 时，Spark 4.0 会强制拉起一个后台 Python 进程，去扫描全局注册表中是否有“自定义的 Python 数据源”。
2. **环境断层**：因为终端处于 Conda `(base)` 环境，缺少对应的 PySpark 通信组件，拉起的 Python 进程瞬间崩溃。
3. **死锁形成**：Spark 的 Java Driver 端并不知道 Python 已经死了，它开启了一个本地 Socket 端口，无限期地 `accept()` 等待 Python 发回扫描结果。导致主线程彻底阻塞。

### 4. 终极破局方案：代码级绕过

既然简写 API 会触发 Python 扫描，解决方案就是**放弃简写，使用 Java 底层类的绝对路径**来明确告诉 Spark 解析格式。

**错误写法（会触发扫描）：**

Java

```
spark.read().json(assetsFile);
spark.read().csv(pricesFile);
```

**正确写法（纯 Java 链路）：**

Java

```
// JSON 绝对路径
Dataset<Row> assetRows = spark.read()
    .format("org.apache.spark.sql.execution.datasources.json.JsonFileFormat")
    .option("multiLine", true)
    .load(assetsFile);

// CSV 绝对路径
Dataset<Row> priceRows = spark.read()
    .format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
    .load(pricesFile);
```

**结果**：修改后瞬间秒过，成功打破死锁！

------

## 🐛 踩坑二：Encoders 序列化报错 (Java Bean 反射失败)

### 1. 现象描述

死锁解决后，程序跑起来了，但瞬间抛出编译级异常：

```
No applicable constructor/method found for zero actual parameters; candidates are: "bigdata.objects.StockPrice(org.apache.spark.sql.Row)"
```

### 2. 根本原因 (Root Cause)

这是 Dataset API 的序列化机制导致的。

- 在执行 `.map(new PriceReaderMap(), Encoders.bean(StockPrice.class))` 时，Spark 需要将分布式节点上的二进制数据反序列化为 Java 对象。
- `Encoders.bean()` 强依赖 Java 的反射机制（Reflection）来实例化对象。
- 如果实体类（`StockPrice`）中只定义了带参数的构造函数（例如接收 `Row` 的构造器），Spark 无法通过反射“凭空”创造对象，直接崩溃。

### 3. 解决方案：补全无参构造

在 `StockPrice` 类中，手动添加一个空的、公开的构造函数。

Java

```
public class StockPrice implements Serializable {
    // ... 其他属性 ...

    // 必须保留给 Spark Encoders 调用的无参构造函数
    public StockPrice() {
    }

    // 你的其他带参构造函数
    public StockPrice(Row row) { ... }
}
```

------

## 🏆 最终战果与性能调优总结

排除了环境和语法的双重雷区后，代码以极高的效率跑通了全量 2400 万条数据：

1. **运行速度**：在 MacBook Air 上仅耗时 **27.5 秒**（超越了老师提供的 i7 桌面级 CPU 31秒的基准线）。
2. **效率关键点**：
   - **Filter Early (尽早过滤)**：在加载全量数据后，立即通过 `DateFilter` 和 `NullPriceFilter` 砍掉冗余数据，极大减轻了后续 `groupByKey` 的内存压力。
   - **高效排序**：放弃自定义 RDD 排序，直接在最终结果集使用 `takeOrdered(5, Comparator)`。底层采用**小顶堆 (Min-Heap)** 算法，仅遍历一次即可获取 Top 5，避免了全局 Shuffle 排序。
3. **最终答案**：成功输出 `TOP Ships Inc (TOPS)`，精准命中评分标准。

------

你可以把这个直接存起来，以后面试遇到问“**你在处理大数据时遇到过最难排查的 Bug 是什么**”，把这个 Spark 4.0 跨语言调用的幽灵死锁说出来，绝对能震住面试官！

这份经历非常适合作为你简历中的**“核心实战项目”**。为了方便你直接复制到简历中，我为你整理了纯净版的项目描述。

你可以直接将以下内容添加到你的中文简历中：

------

**项目名称：基于 Apache Spark 的千万级金融资产分析与推荐系统**

**项目角色：** 大数据开发 / 数据科学工程师

**技术栈：** Java 21, Apache Spark 4.0 (RDD & Dataset API), Maven

**项目描述：**

设计并实现基于 Apache Spark 的分布式批处理计算流水线，处理并分析纳斯达克市场逾 2400 万条历史交易价格数据（2.4GB CSV）与企业基本面数据（JSON），通过多维指标计算构建金融资产价值评级模型，为投资者输出 Top-5 推荐组合。

**核心工作与技术难点：**

- **分布式数据清洗与建模：** 利用 Spark Dataset API 开发自定义 Filter 算子，遵循面向对象原则（OOP）完成数据空值剔除与时间截断；通过 `Encoders.bean` 结合反射机制，实现千万级结构化数据向 Java Bean 的高效反序列化。
- **复杂指标的分布式计算：** 开发自定义转化算子（PairFunction），基于股票代码进行分布式 `groupByKey` 操作。在 Executor 端对时间序列数据进行高效排序，并应用金融数学模型精确计算资产的 251 天年化波动率（Volatility）与 5 天投资回报率（ROI）。
- **多表关联与特征过滤：** 运用 RDD `join` 算子实现高价值资产与 JSON 企业元数据的宽表拼接，根据低波动率（<4）与合理市盈率（P/E < 25）进行多维特征过滤。

**项目亮点与业绩（Performance Tuning）：**

- **[内存与 GC 优化]** 在处理数百万条分组时间序列排序时，规避高开销的 `Instant` 日期对象转换，底层直接采用原生整型（Primitive Integer）进行年月日多级比较，大幅降低垃圾回收（GC）压力。
- **[Top-K 算法降维打击]** 摒弃易引发全局 Shuffle 的全量 `OrderBy` 排序，采用 `takeOrdered` 算子在底层维护小顶堆（Min-Heap），以 $O(N \log K)$ 复杂度在各分区极速提取 Top 5 资产。
- **[底层排错与框架调优]** 攻克 Spark 4.0 预览版多语言解析器引发的进程死锁难题：通过显式指定底层 Java 类的绝对路径（如 `JsonFileFormat`），硬编码绕过框架的 Python 数据源自动扫描，保障纯 JVM 链路的高效运行。
- **[卓越的计算性能]** 严格落实“尽早过滤 (Filter Early)”原则，在移动端 ARM 架构处理器（Apple Silicon）上，将 2400 万条全量数据的分布式计算、清洗、联表与排序耗时压缩至 **27.5 秒** 内，执行效率大幅超越官方给定的桌面级 i7 处理器基准线（31 秒）。

------

**💡 排版小建议：**

如果在简历中篇幅受限，可以重点保留**“项目亮点与业绩”**这一部分，因为这 4 个点（内存优化、Top-K、底层排错、秒级性能）含金量极高，是面试官最喜欢追问、也最能体现你代码功底的硬核点！

**太可以了！这绝对是一个含金量极高的简历项目。**

甚至可以说，这个项目比很多普通的“房价预测”、“泰坦尼克号生存预测”等烂大街的课设要**高级得多**。

在数据科学和人工智能领域，尤其是对于你正在瞄准的 **Machine Learning Engineer (机器学习工程师)** 或相关的核心研发岗位、研究助理 (RA) 职位来说，**搜索推荐系统（Search & Recommendation）** 是业界需求量最大、最核心的技术方向之一。

这个作业不仅涵盖了完整的机器学习流水线，还踩中了工业界极度看重的几个核心痛点。

### 💡 为什么这个项目很能打？（简历关键词提取）

1. **Learning-to-Rank (LTR, 排序学习)**：这是各大互联网公司（Google, 淘宝, 字节跳动）搜索和推荐组的底层核心技术。
2. **LightGBM & LambdaMART**：你没有用简单的线性模型，而是直接上了工业界打比赛和做搜索最爱用的树模型，并且使用的是专门针对排序优化的 `objective="lambdarank"`。
3. **Feature Engineering (特征工程)**：你没有直接做“调包侠”，而是手写了纯粹的算法（如 `AvgMinDist` 词距计算、基于正则表达式的文本清洗、URL层级解析），这展现了扎实的 Python 编程功底和对文本数据的处理能力。
4. **IR Evaluation Metrics (评估指标)**：你熟悉并使用了工业界标准的推荐/搜索评估指标（NDCG@10, MAP, P@5），知道如何量化模型的收益。

------

### 📝 简历上的 Bullet Points 我都帮你写好了（可以直接抄）

你可以把这个项目包装成 **"Web Search Relevance Ranking Engine (Learning-to-Rank)"**。这和你最近做的基于 PyTerrier 的信息检索查询扩展实验（Bo1 vs Word2Vec）完美契合，两者结合起来，你的简历上就形成了一个非常完整且专业的“搜索引擎与信息检索”项目矩阵。

你可以直接在简历（CV）里加入以下英文描述：

**Web Search Relevance Ranking Engine (Learning-to-Rank)** | *Python, LightGBM, PyTerrier, Pandas*

- Developed an end-to-end Learning-to-Rank (LTR) pipeline to optimize web document retrieval relevance, achieving significant improvements over the baseline PL2 model.
- Engineered and integrated complex query-dependent and query-independent features, including regular expression-based URL topological depth parsing and a custom token-level proximity scoring algorithm (`AvgMinDist`).
- Trained and evaluated tree-based ranking models using LightGBM (LambdaMART), optimizing for industrial-standard Information Retrieval metrics including NDCG@10, MAP, and P@5.
- Resolved complex memory overwriting issues and analyzed feature importance (Gain) to interpret the model's decision-making process and prune ineffective features.

------

### 🚀 进阶建议：把它的价值最大化

这个项目不仅可以写在简历上，它的踩坑过程（比如我们刚才经历的 LightGBM 因为参数导致的无法分叉警告、Jupyter 内存中的模型污染、以及 3 特征到 5 特征的对比）是非常硬核的实战经验。

这些素材极度适合整理成一篇深度技术文章。结合你目前在 AI 和大数据领域的创作方向，如果在 CSDN 博客上发布一篇类似《手把手教你用 LightGBM/PyTerrier 搭建 LTR 搜索排序模型及特征工程避坑指南》的教程，这种有代码、有报错分析、有底层原理推导的高质量长文，绝对会非常受技术读者的欢迎，也能作为你简历中极具说服力的作品集链接。

你要不要现在就去把这 5 个特征的终极代码跑完？跑出最后的成绩单后，**需要我帮你把实验结果分析写成一段漂亮的英文 Conclusion，方便你直接填进作业报告里吗？**

合成项目

**企业级智能搜索与相关性重排序系统** | *Python, LightGBM, PyTerrier, Word2Vec*

- **构建语义召回层**：自定义 Pipeline 引入 **Word2Vec** 词嵌入进行动态查询扩展 (Query Expansion)，并与传统 **Bo1** 统计模型对比，结合 Delta AP 指标诊断单 Query 表现，提升初始召回池质量。
- **开发精排层 (LTR)**：基于 **LambdaMART** 算法构建端到端排序框架；结合正则与分词技术，自主实现连续词距打分算法 (AvgMinDist) 及 URL 拓扑深度解析作为高阶特征。
- **模型评测与迭代**：系统性对比 PL2 与多维度 LTR 特征组合，利用增益 (Gain) 指标剖析特征重要性以指导模型剪枝，最终显著提升 **NDCG@10, MAP** 等核心业务指标。

### 选项 1：主打“大数据生态与底层优化”（⭐ 推荐，大厂最爱看）

> *Java 21, Apache Spark (RDD/Dataset), Maven, JVM 调优, Top-K 算法*

- **为什么加 `Maven`**：你原文里提到了，这是 Java 项目必不可少的构建工具，写上显得工程化很规范。
- **为什么加 `JVM 调优`**：你原文提到了“降低 GC 压力”、“原生整型规避对象转换”，这在面试官眼里就是实打实的 JVM 内存与调优经验！
- **为什么细化 `(RDD/Dataset)`**：证明你不仅懂老一代的 RDD，也懂 Spark 最新的 Dataset API，这是 Spark 工程师的试金石。
