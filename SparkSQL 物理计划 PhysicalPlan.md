---
typora-copy-images-to: images
---

## 物理计划转换过程

SparkSQL 中，物理计划用 SparkPlan 表示，从 Optimized LogicalPlan 传入到 SparkSQL 物理计划提交并执行，主要经过3 个阶段。这3个阶段分别产生 Iterator [PhysicalPlan］、
SparkPlan 、Prepared SparkPlan ，其中 Prepared SparkPlan 可以直接提交并执行（注：这里的PhysicalPlan ”和“SparkPlan”均表示物理计划）。

![1606287372100](.\images\1606287372100.png)

这 3个阶段所做的工作分别如下：

- 由 SparkPlanner 将各种物理计划策略（Strategy ）作用于对应的 LogicalPlan 节点上，生成SparkPlan 列表（注： LogicalPlan 可能产生多种 SparkPlan）；
- 选取最佳的 SparkPlan ，在 Spark 2 版本中的实现较为简单，在候选列表中直接用 next()方法获取第一个。
- 提交前进行准备工作，进行一些分区排序方面的处理，确保 SparkPlan 各节点能够正确
  执行，这步通过 prepareForExecution （）方法调用若干规则（Rule ）进行转换。

## SparkPlanner机制：SparkPlan 生成

Spark SQL 中，当逻辑计划处理完毕后，QueryExecution种会使用 SparkPlanner的 plan （） 方法对LogicalPlan 进行处理，得到对应的物理计划 。实际上， 一个逻辑计划可能会对应多个物理计划，因此， SparkPlanner 得到的是一个物理计划的列表 (Iterator SparkPlan ］）。

![1606294493027](.\images\1606294493027.png)

**SparkPlanner 继承自 SparkStrategies 类，而 SparkStrategies 类则继承自 QueryPlanner 基类，重要的 plan()方法实现就在 QueryPlanner 类中 SparkStrategies 类本身不提供任何方法，而是在内部提供一些SparkPlanner 会用到的各种策略（ Strategy）实现 最后 ，在 SparkPlanner 层面将这些策略整合在一 起，通过 plan （）方法进行逐个应用。**

![1606294593970](.\images\1606294593970.png)

类似逻辑计划阶段的 Anaylzer和Optimizer, SparkPlanner 本身只是一个逻辑的驱动，各种策略的 apply方法把逻辑执行计划算子映射成物理执行计划算子 。

QueryPlanner类中的plan方法，传入 LogicalPlan 作为参数，将Strategies 应用LogicalPlan ，生成物理计划候选集合（Candidates）。如果该集合中存在 PlanLater 类型的SparkPlan 通过 aceholder 中间变量取出对应的 LogicalPlan 后，递归调用plan （）方法，将PlanLater 替换为子节点的物理计划。最后，对物理计划列表进行过滤，去掉一些不够高效的物理计划。

![1606294901979](.\images\1606294901979.png)

具体的转换过程，可查看[ SparkSQL 源码解析 SparkPlanner_](https://blog.csdn.net/qq_41775852/article/details/105206379)

## 执行前的准备： prepareForExecution

物理计划的生成意味着用户的 SQL 语句已经成功转换为 SparkPlan 物理算子树。然而，在通常情况下，到了这一步仍然不能直接提交给 Spark 系统执行，还需要完成若干的准备工作，对树型结构的物理计划进行全局的整合处理或优化。在QueryExection 中，最后阶段由 prepareforExecution 方法对传入的 SparkPlan 进行处理而生成 executedPlan ，处理过程仍然基于若干**规则**。

![1606295620286](.\images\1606295620286.png)

![1606295638409](.\images\1606295638409.png)

![1606360026339](.\images\1606360026339.png)

### EnsureRequirements 规则

**EnsureRequirements 用来确保物理计划能够执行所需要的前提条件，包括对分区和排序逻辑的处理**。SparkPlan 对输入数据的分布(Distribution ）情况和排序（ Ordering ）特性有着一定的要求 例如， SortMerge 类型的 Join 子，要求输入数据已经按照 Hash 方式分区且处于有序状态。

**如果输入数据的分布或有序性无法满足当前节点的处理逻辑，则 EnsureRequirements 规则会在物理计划中添加一些 Shuffle 操作或排序操作来达到要求，体现在物理算子树上就是加入Exchange 节点或 SortExec 节点**。此外，该过程还涉及依赖信息（ShuffleDependcency ）的创建、ShuffledRowRDD 的构造等。

EnsureRequirements 规则的 apply 方法可知，在遍历 SparkPlan 的过程中，**当匹配到 Exchange 节点（ShuffleExchange ）且其子节点也是 Exchange 类型时，会检查两者的 Partitioning方式，判断能否消除多余的 Exchange 节点**。除此情况外，**遍历过程中会逐个调用 ensureDistributionAndOrdering 方法来确保每个节点的分区与排序需求**。 

因此， EnsureRequirements 规则的核心逻辑体现在 ensureDistributionAndOrdering 方法：

**（1）添加Exchange 节点**

Exchange 本身也是 UnaryExecNode 类型的 SparkPlan ，在 Spark SQL 中被定义为抽象类。继承 Exchange 的子类有 BroadcastExchangeExec和ShuffleExchange 两种。很明显，ShuffleExchange 会通过 Shuffle 操作进行重分区处理，而 BroadcastExchangeExec 则对应广播操作。

![1606360857834](.\images\1606360857834.png)

需要添加 Exchange 节点的情形有以下两种：

- **数据分布不满足**：：**子节点的物理计划输出数据无法满足（Satisfies ）当前物理计划处理逻辑中对数据分布的要求**，例如子节点输出数据分布为 UnspecifiedDistribution ，而当前物理计划对输入数据分布的需求是 OrderedDistribution。
- **数据分布不兼容**：当前物理计划为 BinaryExecNode 类型，即存在两个子物理计划时，**两个子物理计划的输出数据可能不兼容**（Compatile 例如， Hash 的方式不同，导致应该在同一个分区的数据最终落到不同的节点上 在这种情况下，也需要创建 Exchange 节点重新进行 Shuffie 操作。

在ensureDistributionAndOrdering 方法中，添加 Exchange 节点的过程可以细分为两个阶段，分别针对单个节点和多个节点。

- 第一个阶段是判断每个子节点的分区方式是否可以满足（Satisfies ）对应所需的数据分布。如果满足，则不需要创建 Exchange 节点。否则根据是否广播来决定添加何种类型的 Exchange 节点。
- 第二个阶段专门针对多个子节点的情形，如果当前 SparkPlan 节点 要所有子节点分区方式兼容但并不能满足时，就需要创建 ShuffieExchange 节点。例如， SortMerge 类型的 Join 节点就需要两个子节点的 Hash 方式相同。例如，SortMerge 类型的 Join 节点中 requiredChildDistribution 列表为［ClusteredDistribution(le Keys), ClusteredDistribution(rightKeys ）］，假设两个子节点的 Partitioning 都无法输出该数据分布，那么
  就会添加两个 ShuffleExchange 节点。

**（2）应用 ExchangeCoordinator 协调分区**

ExchangeCoordinator 用来确定物理计划生成的 Stage 之间如何进行Shuffle的行为。顾名思义，**其作用在于协助 ShufileExchange 节点“更好地”执行**。Spark-2.1版本中，ExchangeCoordinator 的功能比较简单，**仅用于确定数据 shuffle 后（ Post-shuffle ）的分区数目。**

ExchangeCoordinator 是ShuflleExchange 的Option 类型构造参数，属于“可有可无”的，默认情况下不会被创建。根据 withExchangeCoordinator 方法的逻辑，需要满足两个条件：

- Spark SQL 的自适应机制开启，对应的参数（spark.sql.adaptive enabled ）设置为 true
- 这批 SparkPlan 节点能够支持协调器，一种情况是至少存在 ShuffleExchange 类型的节点且所有节点的输出分区方式都是 HashPartitioning。另一种情况是节点数目大于 1且每个节点输出数据的分布都是 ClusteredDistribution 类型。

当 ShuffleExchange 中加入了 ExchangeCoordinator 来协调分区数目时，需要知道子物理计划输出的数据统计信息，因此在协调之前需要将 ShuffleExchange 之前的 Stage交到集群执行来获取相关信息。

**(3 ）添加 SortExec 节点**

排序的处理在分区处理（创建完 Exchange ）之后，其逻辑相对简单，不用考虑子节点彼此之间的兼容问题，只需要对每个子节点单独处理。**当且仅当所有子节点的输出数据的排序信息满足当前节点所需时，才不需要添**
**SortExec 节点；否则，需要在当前节点上添加 SortExec 为父节点**。



至此， Ensurerequirements规则的处理逻辑结束，调用 TreeNode 中的 withNewChildren将 SparkPlan 中原有的子节点替换为新的子节点。ShuffleExchange 执行得到的 RDD 称为 ShuffledRowRDD，其生成过程：ShuffleExchange 执行 do Execute 时，首先会创建 ShuffleDependency ，然后根据 ShuffleDependency
构造 ShuffledRowRDD ，其中的重点在于 ShuffleDependency 的创建，分为以下两种情况：

- 包含 ExchangeCoordinator 协调器：如果需要 ExchangeCoordinator 协调 ShuffledRowRDD的分区，则需要先提交该 ShuffleExchange 之前的 Stage到 Spark 集群执行，完成之后确定ShuffledRowRDD 的分区索引等信息。
- 直接创建：直接执行 prepareShuffieDependency 方法来创建 RDD 的依赖，然后根据 ShuffleDependency 创建 ShuffiedRowRDD 对象。在这种情况下， ShuffiedRowRDD 中每个分区ID和Exchange 节点进行Shuffle 操作后的数据分区是一对一的映射关系。

