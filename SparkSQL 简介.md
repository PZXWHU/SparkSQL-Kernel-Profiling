## SQL-on-Hadoop

SQL-on-Hadoop 解决方案从架构上来看可以简单地划分为三层结构 ：

- 最上层是**应用（语言）层**，应用层为用户提供数据管理查询的接口，不同的 SQL-on-Hadoop 系统往往提供各自的 SQL 语法特性，如 Hive HiveQL, Pig PigLatin ，Spark SQL DataFrame。
- 应用层之下是**分布式执行层**， SQL-on-Hadoop 系统通过一定的规则或策略将 SQL 语句转换为对应的计算模型。除 MapReduce Spark 等通用的计算框架外，分布式执行层可能是某些系统自定义的计算单元，例如 Impala 中的 Query Exec Engine分布式执行层通过接口访问数据存储层中的数据，并完成相应的计算任务 。
- SQL on-Hadoop统的底层是**数据存储层**，主要负责对关系数据表这样的逻辑视图进行存储与管理 目前，各种SQL-on-Hadoop 数据存储层基本都支持分布式文件系统 HDFS 和分布式 NoSQL 数据库。

总的来看， SQL-on-Hadoop 解决方案类似“堆积木”，各层之间松搞合，并可以灵活组合 数据存储层与分布式执行层之间通过特定的数据读写接口进行数据的交互 这种分层解辑的方式 方面具有通用性（各层可以分别分离为子系统）和灵活性（彼此能够互相组合）的优势，另一方面隔离了各层的特性，限制了深度集成优化的空间。



## RDD、Dataframe和Dataset

DataFrame 和RDD一 样，都是不可变分布式弹性数据集 不同之处在于， RDD 中的数据不包含任何结构信息，数据的内部结构可以被看作“黑盒”，因此，直接使用 RDD 时需要开发人员实现特定的函数来完成数据结构的解析， **DataFrame 中的数据集类似于关系数据库中的表，按列名存储，具有 Schema 信息**，开发人员可以直接将结构化数据集导入 DataFrame 。**DataFrame的数据抽象是命名元组（对应 Row 类型），相比 RDD 多了数据特性，因此可以进行更多的优化。**

Dataset 是比 DataFrame 更为强大的 API ，两者整合之后 DataFrame 本质上是一种特殊的 Dataset (Dataset[Row］类型） Dataset 具有两个完全不同的 API 特征 强类型
 API 和弱类型API。 强类型一般通过 Scala 中定义的 Case class或者java中的class来指定。

作为 DataFrame 的扩展， Dataset 结合了 RDD DataFrame 的优点，提供类型安全和面向对象的编程接口，并引入了编码器（Encoder ）的概念。**Encoder 不仅能够在编译阶段完成类型安全检查，还能够生成字节码与堆外数据进行交互，提供对各个属性的按需访问，而不必对整个对象进行反序列化操作，极大地减少了网络数据传输的代价**。

**此外， DataFrame Dataset 这些高级的 API ，能够利用 Spark SQL 中的 Optimizer Tungsten 技术自动完成存储和计算的优化，降低内存使用，并极大地提升性能**

