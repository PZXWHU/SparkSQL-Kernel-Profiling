---
typora-copy-images-to: images
---

## TreeNode 体系

TreeNode 类是 SparkSQL 中所有树结构的基类，定义了一系列通用的集合操作和树遍历操作接口。

TreeNode 内部包 Seq[BaseType］类型的变量 **children** 来表示孩子节点。TreeNode 定义了 **foreach map collect** 等针对节点操作的方法，以及 **transformUp、transformDown** 等遍历节点并对匹配节点进行相应转换的方法。TreeNode 本身是 scala.Product类型，因此可以通过 productElement 函数或 productlterator 迭代器对 Case Class 参数信息进行索引和遍历。实际上， TreeNode 一直在内存里维护，不会 dump 到磁盘以文件形式存储，且无论在映射逻辑执行计划阶段，还是优化逻辑执行计划阶段，树的修改都是以替换已有节点的方式进行的。

![1606213106396](.\images\1606213106396.png)

TreeNode 提供的仅仅是 一种泛型，实际上包含了两个子类继承体系，即图中的 **QueryPlan**
**和Expression 体系** 。Expression是 Catalyst 中的表达式体系，**QueryPlan 类下面又包含逻辑算子树（LogicalPlan ）和物理执行算子树（SparkPlan ）两个重要的子类**。

![1606213123873](.\images\1606213123873.png)

除上述操作外， Catalyst 中还提供了节点位置功能，即能够**根据 TreeNode 定位到对应的**
**SQL 字符串中的行数和起始位置**， 该功能在 SQL 解析发生异常时能够方便用户迅速找到出错的地方。

## Expression体系

表达式一般指的是**不需要触发执行引擎而能够直接进行计算的单元**，例如加减乘除四则运算、逻辑操作、转换操作、过滤操作等。Catalyst 实现了完善的表达式体系，与各种算子（ QueryPlan ）占据同样的地位 。**算子执行前通常都会进行“绑定”操作，将表达式与输入的属性对应起来，同时算子也能够调用各种表达式处理相应的逻辑**。

### Expression属性、操作

Expression类中定义了5类操作：**基本属性、核心操作、输入输出、字符串表示和等价性判断**。

1. 核心操作
   - eval 函数实现了表达式对应的处理逻辑，也是其他模块调用该表达式的主要接口。
   - genCode和 doGenCode 用于生成表达式对应的 java代码（代码生成）。
2. 字符串表示：用于查看该 Expression 的具体内容，如表达式名和输入参数等
3. 基本属性
   - foldable：该属性用来标记表达式能否在查询执行之前直接静态计算。
   - deterministic：该属性用来标记表达式是否为确定性的，即每次执行 eval 函数的输出是否都相同。
   - nullable ：该属性用来标记表达式是否可能输出 Null 值。
4. 等价性判断
   - canonicalized：返回经过规范化（Canonicalize ）处理后的表达式。
   - semanticEquals：：判断两个表达式在语义上是否等价。基本的判断条件是两个表达式都是确定性的（ deterministic=true ）且两个表达式经过规范化处理后（Canonicalized ）仍然相同。
5. 输入输出
   - references：返回值为 AttributeSet 类型，表示该 Expression 中会涉及的属性值，默认情况为所有子节点中属性值的集合。

![1606213154214](.\images\1606213154214.png)

### Expression分类

Expression 本身也是 TreeNode 类的子类，因此能够调用所有 TreeNode方法，例如 transform 等，**也可以通过多级的子 Expression 组合成复杂的 Expression**。

![1606214018430](.\images\1606214018430.png)

- Nondeterministic 接口：具有不确定性的 Expression ，其中 deterministic、 foldable 属性都默认返回 false。
- Unevaluable 接口：非可执行的表达式，即调用其 eval 函数会抛出异常。
- CodegenFallback 接口：不支持代码生成的表达式 某些表达式涉及第三方实现（例如 HiveUDF ）等情况，无法生成 Java 代码，此时通过 CodegenFallback 直接调用，该接口中实现了具体的调用方法。
- LeafExpression：：叶子节点类型的表达式，即不包含任何子节点，因此其 children 方法通常默认返回 Nil。
- UnaryExpression ：一元类型表达式，只含有一个子节点。
- BinaryExpression：二元类型表达式，包含两个子节点。
- TernaryExpression ：三元类型表达式，包含 三个子节点。

## 数据类型体系

![1606214514652](.\images\1606214514652.png)

## QueryPlan体系

### QueryPlan属性、操作

QueryPlan 的主要操作分为6个模块，分别是**输入输出、 字符串、 规范化、表达式**
**操作、基本属性和约束**。



1. 输入输出
   - output 是返回值为 Seq[Attribute]的虚函数，具体内容由不同子节点实现
   - 而 outputSet 是将 output 的返回值进行封装，得到AttributeSet 集合类型的结果
   -  inputSet是获取输入属性的方法，其返回值也是 AttributeSet,**节点的输入属性对应所有子节点的输出**；
   - producedAttributes 表示该节点所产生的属性；
   - missinglnput 表示该节点表达式中涉及的但是其子节点输出中并不包含的属性
2. 基本属性
   - schema 对应 output 输出属性的schema 信息
   - allAttributes 记录节点所涉及的所有属性（ Attribute ）列表
   - aliasMap 记录节点与子节点表达式中所有的别名信息，
   - references 表示节点表达式中所涉及的所有属性集合
   - subqueries和innerChildren 都默认实现该 QueryPlan 节点中包含的所有子查询
3. 字符串
   - 这部分方法主要用于输出打印 QueryPlan 树型结构信息，其中 schema 信息也会
     以树状展示。
   - 需要注意的一个方法是 statePrefix，用来表示节点对应计划状态的前缀字符串。如果该计划不可用 (invalid ），则前缀会用感叹号（“！”）标记。
4. 规范化：：类似 Expression 中的方法定义，对 QueryPlan 节点类型也有规范化（Canonicalize)的概念 ，QueryPlan 默认实现中， canonicalized 直接贼值为当前的 QueryPlan 类。
5. 约束：本质上也属于数据过滤条件（ filter ）的一种，同样是表达式类型。对于显式的过滤条件，约束信息可以“推导”出来，例如，对于“ a > 5”这样的过滤条件，显然a的属性不能为 null ，这样就可以对应地构造 isNotNull (a ）约束。在实际情况下， SQL 语句中可能会涉及很复杂的约束条件处理，如约束合并、等 价性判断等。



![1606214770018](.\images\1606214770018.png)

## LogiacalPlan体系

### LogiacalPlan属性、操作

**成员变量：**

- resolved：用 来标记 LogicalPlan 是否为经过解析的布尔类型值；
- canonicalized：重载了 QueryPlan 中的对应赋值，默认实现消除了子查询别名之后的 LogicalPlan。

**方法：**

- childrenResolved ：标记子节点是否已经被解析
- statePrefix ：重载了 QueryPlan 中的实现，如果该逻辑算子树节点未 经过解析，则输
  出的字符串前缀会加上单引号（‘）。
-  isStreaming ：用来表示当前逻辑算子树中 否包含流式数据源；
- statistics ：记录了当前节点的统计信息
- maxRows ：记录了当 前节点可能计算的最大行数， 一般常用于 Limit 算子
- refresh ：会递归地刷新当前计划中的元数据等信息
- resolveXX：用来执行对数据表 、表达式、 schema列属性等类型的解析。

![1606216067050](.\images\1606216067050.png)

### LogicalPlan分类

LogicalPlan 仍然是抽象类，根据子节点数目，绝大部分的 LogicalPlan 可以分为
3类，即叶子节点 LeatNode 类型（不存在子节点）、 一元节点 UnaryNode 类型（仅包含 一个子节点）、二元节点 BinaryNode 类型（包含两个子节点）和直接继承于LogicalPlan的逻辑算子节点。

![1606217204782](.\images\1606217204782.png)

#### LeafNode

![1606216175923](.\images\1606216175923.png)

**LeafNode 类型的 LogcalPlan 节点对应数据表和（Command ）相关的逻辑**，因此这些 LeafNode 子类中有很大部分都属于 datasources 包和command包。实现 Runnable Command 特质（Trait ）的类共有40多个，是数量最多为庞大的 LogicalPlan 类型 ，顾名思义， **RunnableCommand 是直接运行的命令，主要涉及 12种情形，包括 Database 相关命令、 Tabl 相关命令、 View 相关命令 DDL 关命令、 Function命令和 Resource 关命令**等， 例如， Database 相关命令有两个，分别是 showDatabasesCommand、SetDatabaseCommand ，用于显示当前数据库名称和切换当前数据库到相应的数据库。

#### UnaryNode



常见于对数据的逻辑转换操作，包括过滤等。

![1606216548504](.\images\1606216548504.png)

- 用来定义重分区（repartitioning ）操作的3个 UnaryNode。即 RedistributeData 及其两个子类SortPartitions 和RepartitionByExpression ，主要针对现有分区和排序的特点不满足的场景。
- 脚本相关的转换操作 ScriptTransformation ，用特定的脚 本对输入数据进行转换。
- Object相关的操作，即ObjectConsumer 这个特质（Trait ）和其他 10 个类。
- 基本操作算子（basicLogicalOperators ），数量最多，共有 19 种，涉及 ProjectFilter Sort等各种常见的关系算子。

#### BinaryNode

![1606217085979](.\images\1606217085979.png)

#### 其他类型的 LogicalPlan

​	![1606217257131](.\images\1606217257131.png)

## SparkPlan体系

Spark SQL 最终将 SQL 语句经过逻辑算子树转换成物理算子树。叶子类型的 SparkPlan 节点负责呗无到有”地创建 RDD，每个非叶子类型的 SparkPlan 节点等价于在一次RDD 上进行 Transformation ，**即通过调用 execute （）函数转换成新的 RDD**。SparkPlan 在对 RDD Transformation 的过程中除对数据进行操作外，还可能对 RDD 的分区做调整 。此外， SparkPlan 除实现 execute 方法外，还有一 种情况是直接执行executeBroadcast 方法，将数据广播到集群上。

![1606288342398](.\images\1606288342398.png)

SparkPlan 的主要功能可以划分为3大块：

- 每个 SparkPlan 节点必不可少地会记录其元数据（Metadata ）与指标（Metric ）信息，这些信息以 Key-Value 的形式保存在 Map数据结构中，统称为 SparkPlan 的Metadata和 Metric 体系。
- 在对 RDD 进行 Transformation操作时，会涉及数据分区（Partitioning ）与排序（ Ordering ）的处理，称为 SparkPlan 的Partitioning与Ordering 体系。
- SparkPlan 作为物理计划，支持提交到 Spark Core 去执行，即 SparkPlan
  的执行操作部分，以 execute和executeBroadcast 方法为主。

![1606288485427](.\images\1606288485427.png)

### SparkPlan分类

#### LeafExecNode

叶子节点类型的物理执行计划不存在子节点。**物理执行计划中与数据源相关的节点都属于该类型。LeafExecNode 类型的 SparkPlan 负责对初始 RDD 的创建。**例如，RangeExec 会利用 SparkContext 中的 parallelize 方法生成给定范围内的 64 位数据的 RDD,HiveTableScanExec 会根据 Hive数据表存储的 HDFS 信息直接生成 HadoopRDD, FileSourceScanExec 根据数据表所在的源文件生成 FileScanRDD。

![1606288700715](.\images\1606288700715.png)

#### UnaryExecNode

UnaryExecNode 类型的物理执行计划的节点是一元的。**UnaryExecNode 节点的作用主要是对 RDD 进行转换操作**。例如，ProjectExec和FilterExec 分别对子节点产生的 RDD 进行列剪裁与行过滤操作。Exchange 负责对数据进行重分区， SampleExec 对输入 RDD 中的数据进行采样， SortExec 按照一定条件对输入 RDD 中数据进行排序， holeStageCodegenExec 类型的 SparkPlan 将生成的代码整合成单个 Java 函数。

![1606288892053](.\images\1606288892053.png)

#### BinaryExecNode

BinaryExecNode 类型的 SparkPlan 具有两子节点，这种二元类型的物理执行计划在 SparkSQL 中共定义了6种。这些 SparkPlan 中除 CoGroupExec 外，其余的5种都是不同类型的 Join 执行计划.

![1606288974897](.\images\1606288974897.png)

#### 其他类型的 SparkPlan

![1606289030650](.\images\1606289030650.png)

### Metadata Metrics 体系

**元数据和指标信息是性能优化的基础**， SparkPlan 提供了 Map 类型的数据结构来存储相关信息，以便更加详细地刻画 SparkP!an 的细节。 **默认情况下， SparkPlan 中这两个 Map 的值均为空。**

元数据信息 Metadata 对应 Map 中的 key和 value 都为字符串类型。一 般情况下，**元数据主要用于描述数据源的 一些基本信息**，例如数据文件的格式、存储路径等 目前只有FileSourceScanExec和RowDataSourceScanExec 两种叶子节点类型的 SparkPlan 对其进行了重载实现。

指标信息 Metrics 对应 Map 中的 key 为字符串类型，而 value 部分是 SQLMetrics 类型。在Spark 执行过程中， Metrics 能够记录各种信息，**为应用的诊断和优化提供基础**。

在对 Spark SQL 进行定制肘，用户可以自定义一些指标，并将这些指标显示在 UI 上。一方面，定义越多的指标会得到越详细的信息；另一方面，指标信息要随着执行过程而不断更新，会导致额外的计算，在 一定程度上影响性能

### Partitioning与Ordering 体系

Partitioning和Ordering 体系可以概括为“承前启后”。“承前”体现在对输入数据特性的需求上，**requiredChildDistribution和requiredChildOrdering 分别规定了当前 SparkPlan所需的数据分布和数据排序方式列表，本质上是对所有子节点输出数据（RDD ）的约束**。“启后”体现在对输出数据的操作上， **outputPartitioning 定义了当前 SparkPlan 对输出数据（RDD)的分区操作， outputOrdering 则定义了每个数据分区的排序方式**。

![1606290165103](.\images\1606290165103.png)

#### Distribution和 Partitioning 的概念

SparkPlan 分区体系实现中， Partitioning 表示对数据进行分区的操作， Distribution 则表示数据的分布。在 SparkSQL Distribution Partitioning 均被定义为接口。

![1606290330179](.\images\1606290330179.png)

Distribution 定义了查询执行时，**同一个表达式下的不同数据元组（Tuple ）在集群各个节点上的分布情况**。Distri bution 可以用来描述以下两种不同粒度的数据特征。

- 节点间（Inter-node ）分区信息，即数据元组在集群不同的物理节点上是如何分区的。
- 分区数据内（Intra-partition ）排序信息，即单个分区内数据是如何分布的。

Spark 2.1版本中包括以下5种 Distribution 的实现：

- UnspecifiedDistribution ：未指定分布，无需确定数据元组之间的位置关系。
- AllTuples ：只有 一个分区，所有的数据元组存放在一起（ Co-located）
- BroadcastDistribution ：广播分布，数据会被广播到所有节点上。构造参数 mode 为广播模式
  (BroadcastMode ）
- ClusteredDistribution：：构造参数 是clustering Seq [Expression ］类型，起到了哈希函数的效果，数据经过 clustering 计算后，相同 value 的数据元组会被存放在一起（ Co-located 如果有多个分区的情况，则相同数据会被存放在同一个分区中；如果只能是单个分区，则相同的数据会在分区内连续存放。
- OrderedDistribution：：构造参数 ordering 是Seq[SortOrder ]类型，该分布意味着数据元组会根据 ordering 计算后的结果排序.

**Partitioning 定义了一个物理算子输出数据的分区方式**。具体包括子 Partitionging 之间、目标Partitioning 和Distribution 之间的关系。

Partitioning 接口中包含1个成员变量和3个函数来进行分区操作：

- numPartitions ：指定该 SparkP!an 输出 RDD 的分区数目
- satisfies(required: Distribution ）：当前的 Partitioning 操作能否得到所需的数据分布（Required 当不满足时（结果为 false ）， 一般需要进行 repartition 操作，对数据进行重新组织。
- compatibleWith(other: Partitioning）：：当存在多个子节点时，需要判断不同的子节点的分区操作是否兼容 直观地看，只有当两个 Partitioning 能够将相同 key 的数据分发到相同的分区时，才能够兼容。
- guarantees( other: Partitioning）：：如果 guarantees(B ）能够为真，那么任何 A进行分区操作所产生的数据行也能够被B产生。这样， 就不需要再进行重分区操作， 该方法主要用来避免冗余的重分区操作带来的性能代价。

通常情况下， LeafExecNode 类型的 SparkPlan 会根据数据源本身的特点（包括分块信息和数据有序性特征）构造 RDD 与对应的 Partitioning和Ordering 方式， UnaryExecNode 类型SparkPlan 大部分会沿用其子节点的 Partitioning和Ordering 方式（SortExecNode等本身具有排序操作的执行算子例外），而 BinaryExecNode 往往会根据两个子节点的情况综合考虑，具体可以参SortMergeJoinExec 等执行算子的源码实现。



## Catalog体系

SparkSQL 系统中， Catalog 主要用于**各种函数资源信息和元数据信息（数据库、数据表、**
**数据视图、数据分区与函数等**）的统一管理。Spark SQL的 Catalog 体系涉及多个方面，不同层次所对应的关系如下图:

![1606270057867](.\images\1606270057867.png)

Spark SQL 中的 **Catalog 体系实现以 Session Catalog 为主体，通过 SparkSession**
**(Spark 程序入口）提供给外部调用**。一般一个 SparkSession 对应 一个Session Catalog。 本质上，SessionCatalog 起到了 个代理的作用，对底层的元数据信息、临时表信息、视图信息和函数信息进行了封装。

Session Catalog 的构造参数包括 6部分：除传入 Spark SQL和Hadoop配 置信息 CatalystConf与 Configuration外，还有：

![1606270071323](.\images\1606270071323.png)

![1606270492292](.\images\1606270492292.png)

总体来看， **SessionCatalog 是用于管理上述一切基本信息的入口** ，除上述的构造参数外，其内部还包括一个 **mutable 类型的 HashMap 用来管理临时表信息**，以及 **currentDb 成员变量用来指代当前操作所对应的数据库名称** 。Session Catalog 在Spark SQL 个流程中起着重要的作用，在后续逻辑算子阶段和物理算子阶段都会用到。

## Rule 体系

### Rule

Unresolved LogicalPlan 逻辑算子树的操作（如**绑定、解析、优化** ）中，主要方法都是
基于规则（Rule ）的。**通过 Scala 语言模式匹配机制（Pattern-match ）进行树结构的转换或节点改写** 。Rule是 一个抽象类，子类需要复写 **apply(plan: TreeType ）**方法来制定特定的处理逻辑。

![1606271601560](.\images\1606271601560.png)

Rule主要用于三个地方：

- Unresolved LogicalPlan->Resolved LogicalPlan
- Resolved LogicalPlan -> Optimized LogicalPlan
- SparkPlan -> PreparedSparkPlan

### RuleExecutor

有了各种具体规则后，还需要驱动程序来调用这些规则，在 Catalyst 中这个功能由 RuleExecutor 提供。凡是涉及树型结构的转换过程（**如Analyzer 逻辑算子树分析过程、 Optimizer 逻辑算子树的优化过程和后续物理算子树的生成过程等**），都要实施规则匹配和节点处理，都继承自 RuleExecutor [TreeType］抽象类。

![1606272216825](.\images\1606272216825.png)

RuleExecutor 内部提供了一个 Seq[Batch ］，里面定义的是该 RuleExec utor 的处理步骤。每个Batch 代表一 套规则，配备 一个策略，该策略说明了迭代次数（一 次还是多次）。RuleExecutor的execute(plan: TreeType): TreeType方法会按照 batches 顺序和 batch 内的 Rules 顺序，对传入的 plan里的节点进行迭代处理，处理逻辑由具体 Rule 子类实现。

![1606272432992](.\images\1606272432992.png)

rule(plan)即是调用Rule.apply(plan)方法处理plan。

**RuleExecutor有两个重要子类：Analyzer和Optimizer**，**用于逻辑计划的解析与优化。**

##  Strategy 体系

所有的策略都继承自 GenericStrategy 类，其中定义了 planLater和 apply 方法；**SparkStrategy 继承自 GenericStrategy 类，对其中的 planLater 进行了实现，根据传入的 LogicalPlan 直接生成 PlanLater 节点**（PlanLater 本身也是 SparkPlan 的一种，区别在于doExecute （）方法没有实现，表示不支持执行，所起到的作用仅仅是占位，等待后续步骤处理）。各种具体的 Strategy 都实现了 apply 方法，将传入的 LogicalPlan 转换为 SparkPlan列表。

各种 Strategy 会匹配传入的 LogicalPlan 节点，根据节点或节点组合的不同情形实行一对一的映射或多对一 的映射。

- 一对一 的映射方式比较直观，以 BasicOperators 为例，该Strategy 实现了各种基本操作的转换，其 中列出了大量的映射关系，包括 Sort 对应 SortExec, Union 对应 UnionExec等。
- 多对一 的情况涉及**对多个 LogicalPlan 节点进行组合转换**，这里称为**逻辑算子树的模式匹配**。

![1606293299968](.\images\1606293299968.png)

目前在 Spark SQL 中，逻辑算子树的节点模式共有4种：

- Extra ctEquiJoinKeys ：针对具有相等条件的 Join 操作的算子集合，提取出其中的 Join 条件、左子节点和右子节点等信息。
- ExtractFiltersAndinnerJoins： Inner 类型 Join 操作中的过滤条件，目前仅支持对左子树进行处理。
- PhysicalAggregation ：针对聚合操作，提取出聚合算子中的各个部分，并对一些表达式进行初步的转换。
- PhysicalOperation ：匹配逻辑算子树中 Project和Filter等节点，返回投影列、过滤条件集合和子节点。

比如对于PhysicalOperation模式：如果匹配到 Project、 Filter 、BroadcastHint3种类型之一的 LogicalPlan 时，就会递归查找子节点。若子节点也是这3种类型之一 ，则收集节点中的投影列或过滤条件。依此类推，直到碰到其他类型的 Logica Plan 节点为止。

![1606293911420](.\images\1606293911420.png)