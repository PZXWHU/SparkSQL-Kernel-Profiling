---
typora-copy-images-to: images
---

## Join查询概述

在ANSI SQL 标准中，共有5 种 Join 方式：内连接(Inner ）、全外连接（ FullOuter ）、左外连接（ LeftOuter ）、右外连接（ RightOuter）和交叉连接（ Cross ） 。

目前， Spark SQL 中支持的Join 类型主要包括**Inner、FullOuter、LeftOuter、RightOuter、LeftSemi、LeftAnti 和Cross** 共7 种。

![1606541988255](.\images\1606541988255.png)

![1606547243875](.\images\1606547243875.png)

![1606542328018](.\images\1606542328018.png)



## 文法定义和抽象语法树

在Spark SQL 的ANTLR4 文法文件中，与Join 相关的文法定义如下。可以看到， **Join 语句主要针对的是关系数据表， 一般处于From 子语句中**。

![1606541902816](.\images\1606541902816.png)

该文法有几个关键的信息：在FROM 关键字表示的数据源中，**至少包含一个或多个relation**,以及可能的lateralView （注：本章不涉及该关键字，读者可自行研究） 。**每个relation 包含一个主要的数据表（ relationPrimary）和零个或多个参与Join 操作的数据表（joinRelation ）** 。在joinRelation 中，除参与Join 的数据表外，**比较重要的关键字是Join 的类型（joinType ）和Join 的条件（joinCriteria** ）。

举例：select name, score from student join exam on student .id = exam.studentid

![1606542500949](.\images\1606542500949.png)

对于Join 查询，值得关注的是FromClauseContext 节点。对于上述查询， 图中的第一个TableNameContext 子节点对应文法定义中的relationPrimary ，即student数据表；第二个TableNameContext 子节点对应exam 数据表，在JoinRelationContext 节点下还包含对应Join 类型的JoinTypeContext 子节点和对应Join 条件的JoinCriteriaContext 子节点。

具体来看， **JoinCriteriaContext 子节点本质上是一个表示True 和False 谓词逻辑的表达式节**
**点（ BooleanDefaultContext ）** 。JoinCriteriaContext 子节点内容展开如图所示。在本例中，该表
达式的左、右子表达式分别为“student.id”和“ exam.studentld”，这两个表达式都设置了数据表名，属于DereferenceContext 类型。图中的ComparisonOperatorContext 节点对应列之间的相等关系。

![1606542665254](.\images\1606542665254.png)

## Join 查询逻辑计划

### 从AST 到Unresolved LogicalPlan

与Join有关的逻辑计划生成过程由AstBuilder类定义的visitFromC!ause 方法开始

![1606542854737](.\images\1606542854737.png)

从FromC!auseContext 中得到的relation 是RelationContext 的列表。每个RelationCon text 代表一个通过Join 连接的数据表集合， 每个Relation Context 中有一个主要的数据表（ RelationPrim aryContext）和多个需要Join 连接的表（ JoinRelation Context ）。

![1606542943296](.\images\1606542943296.png)

从上述代码逻辑可知，针对Relation Context 对象列表进行foldLeft 操作，将已经生成的逻辑计划与新的Relation Context 中的主要数据表（ relationPrimary ）结合得到Join 算子（ optiona!Map
方法），然后将生成的Join 算子加入新的逻辑计划中。

**JoinType和Join逻辑算子**

![1606543867597](.\images\1606543867597.png)

![1606543946486](.\images\1606543946486.png)

对于本例来说， From ClauseContext 对应的Relation Context 列表中只有一个元素， relationPrimary为student 数据表。由于初始的Logica!Plan 为null ，所以上述代码中的join 值同样为student 对应的Logica!Plan 。然后调用withJoinRelations 方法，将得到的Logica!Plan 与数据表exam 进行Join 操作。

在visitfromC!auseContext 方法中生成primaryRelation (student 表）对应的Logica!Plan 之后，进入withJoinRelation 方法中对JoinRelationContext 中的表进行处理。

![1606544100537](.\images\1606544100537.png)![1606544106779](.\images\1606544106779.png)

withJoinRelation 方法首先会根据SQL 语句中的Join 类型构造基础的JoinType 对象，然后在此基础上判断查询中是否包含了USING 等关键字，并进行进一步的封装，最终得到一个Join 对象的逻辑计划。

![1606544177967](.\images\1606544177967.png)

### 从UnresoIve LogicalPlan 到Analyzed LogicalPlan

在Analyzer 中，与Join 相关的解析规则有很多，包括ResolveReferences ResolveNaturalAndUsingJoin等。可以看到， ResolveRelations 和ResolveReferences 两条规则产生了影响。ResolveRelations 规则的作用是从Catalog 中找到student 和exam 的基本信息，包括数据表存储格式、每一列列名和数据类型等。ResolveReferences 规则负责解析所有列信息，对于上面的逻辑算子树， ResolveReferences的解析是一个自底向上的过程，将所有UnresolvedAttribute 与UnresolvedExtract Value 类型的表达式转换成对应的列信息。

![1606544805465](.\images\1606544805465.png)

在ResolveReferences 规则中，如果传入的逻辑算子树根节点为Join 类型， 则还存在如下的逻辑来处理Join 中冲突的列。**在dedupRight 方法中，针对存在冲突的表达式会创建一个新的逻辑计划，通过增加别名（ Alias）的方式来避免列属性的冲突。**

在Analyzer 中，还有一个和Join 操作直接相关的ResolveNaturalAndUsingJoin 规则。**该规则将NATUAL 或USING 类型的Join 转换为普通的Join 。其主要处理逻辑是根据Join 两边的输出列信息计算得到总的输出列信息，然后将Project 算子添加到常规的Join 算子上。**

### 从Analyzed LogicalPlan 到Optimized LogicalPlan

逻辑算子树优化的第一阶段就是消除多余的别名，对应EliminateSubqueryAliases 优化规则。其逻辑非常简单，将SubqueryAlias(_, child，＿）节点直接替换为child 节点。

经过别名消除之后，接下来的优化是常用的列剪裁（ Column Pruning ），在上述逻辑算子树中，父节点只需要用到两个数据表中的4 列，因此可以在Relation 节点之后添加新的Project 节点进行列剪裁的操作。

![1606545149944](.\images\1606545149944.png)

优化过程将考虑相关算子中的过滤条件。**对于Join 来讲，其连接条件需要保证两边的列都不为null ，因此会触发InferFiltersFrom Constraints 优化规则**。如图所示， 经过该规则的处理， Join 算子中的连接条件多了两个，分别约束student 表中的ID 和exam表中的studentID不为null 。

![1606545203173](.\images\1606545203173.png)

在Optimizer 阶段，**有一条专门针对Join 算子的PushPredicateThroughJoin 优化规则**。顾名思义，该优化规则所起的作用就是对Join 中连接条件可以下推到子节点的谓词进行下推操作。因为经过上一步的优化规则，逻辑算子树中Join 节点多了两个条件用来判定列不为null ，**这两个条件只涉及单个数据表，因此可以下推到对应的子节点中，这样可以达到尽早过滤数据的效果**。经过P us hPredicateThroughJoin 优化规则后，得到的逻辑算子树如图 所示。

![1606545282290](.\images\1606545282290.png)

一般来讲，**优化阶段会将过滤条件尽可能地下推，因此逻辑算子树中的Filter 节点还会被继续处理。该逻辑对应PushDownPredicate 优化规则。**

![1606545332968](.\images\1606545332968.png)

## Join 查询物理计划

### Join 物理计划的生成

上述逻辑算子树将应用3 个策略：**文件数据源（ FileSource ）策略、Join 选择（ JoinSelection ）策略和基本算子（ BasicOperators ）策略**。FileSource 与BasicOperators这两个策略分别用来转换图中对应的逻辑算子，其中FileSource 策略中还用到了PhysicalOperation 这个匹配模式来合并Relation 上方的Project 与Filter 算子。

![1606545658797](.\images\1606545658797.png)

JoinSelection 策略主要根据Join 逻辑算子选择对应的物理算子。值得一提的是，**在JoinSelection策略中用到了ExtractEquiJoinKeys 匹配模式来提取出Join 算子中的连接条件。**ExtractEquiJoinKeys 模式的主要逻辑如下：如果是等值连接（ Equaljoin ）， 则将左、右子节点的连接key 都提取出来。此外，在ExtractEquiJoinKeys 中还通过other Predicates 记录除EqualTo 和EqualNullSafe 类型外的其他条件表达式，**这些谓词基本上可以在Join 算子执行Shuffie 操作之后在各个数据集上分别处理**。

### Join 物理计划的选取

基于上面的分析，该案例对应生成的物理计划（ SparkPlan ）如图所示。在生成物理计划的过程中， JoinSel ection 根据若干条件判断采用何种类型的Join 执行方式。目前在Spark SQL 中， **Join 的执行方式主要有BroadcastHashJoin Exec、ShuffledHashJoinExec 、SortMergeJoinExec BroadcastNestedLoop JoinExec和CartesianProductExec 这5 种。**

![1606545960785](.\images\1606545960785.png)

基本概念：

- **数据表能否广播**：在两个表的Join 操作中，如果一个数据表的数据量非常小，则可以将这个表广播到另一个表数据所在的所有节点上。在J oinSelection 中通过canBroadcast方法来判断一个数据表对应的逻辑计划能否广播。在Spark SQL 中，可以通过“spark.sql . autoBroadcastJoinThreshold ”参数设置自动广播的阔值（单位为Byte ），当某个表的数据量小于这个阔值时，这个表将自动进行广播操作，该参数默认值为10MB 。
- **Join 操作的BuildSide** ：参与Join 操作的左右两个数据表起到的作用是不一样的，例如，在BroadcastHashJoin 中需要决定广播哪个数据表等。**根据源码可知，只有InnerLike 或RightOuter 类型的Join 时，左表才能够被“构建”。**
- **建立HashMap (BuildLocalHashMap ）**：某些Join 在执行过程中需要创建HashMap 以在内存中保存相关的数据，当数据量大的时候，单个分区上创建HashMap 可能导致内存溢出。**在JoinSelection 中实现了一个比较粗粒度的方法canBuildLocalHashMap 来判断某个逻辑计划能否满足创建本地HashMap 的条件**， 主要思想是当前逻辑计划的数据量小于数据广播阔值与Shuffle 分区数目的乘积。

![1606546619903](.\images\1606546619903.png)

1. 优先级最高的是BroadcastHashJoinExec ，这种Join 执行方式相对来讲效率最高，因此也是最先进行判断的:

   - 能够广播右表（ canBroadcast ）且右表能够“构建”（ canBuildRight ），那么构造参数中传入的是BuildRight 。

   - 能够广播左表（ canBroadcast ）且左表能够“构建”（ canBuildLeft），那么构造参数中传入的是BuildLeft 。

2. 优先级次之的是ShuffledHashJoinExec,以BuildRight 为例,要满足以下条件之一：
   - 配置中优先开启SortMergeJoin 的参数（“spark.sql.join.preferSortMerge Join”)并设置为false ，且右表需要满足能够“构建”（ canBuildRight）和能够建立HashMap ( canBuildLocalHashMap ），同时右表的数据量要比左表的数据量小很多（ 3 倍以上） 。
   - 参与连接的key 具有不可排序的特性（不可排序是指无法比较大小）。
3. 最常见的Join 执行方式就是SortMergeJoinExec，**只需要参与Join 的key 满足可排序的特性即可**。
4. **剩下的情况都是不包含Join 条件的语句**，大致逻辑如下：首先判断是否执行数据表广播操作，对应BuildLeft 和BuildRight 两种情况，生成BroadcastNestedLoopJoinExec 类型的Join 物理算子。如果不满足数据表广播操作，**而Join 类型是InnerLike ，那么就会生成CartesianProductExec类型的Join 物理算子**。如果上述情况都不满足，那么只能选择两个数据表中数据量相对较少的数据表来做广播，同样生成BroadcastNestedLoopJ oinExec 类型的Join 物理算子。

## Join 查询执行

### Join 执行基本框架

在Spark SQL 中， Join 的实现都基于一个基本的流程，如图8 .17 所示。根据角色的不同，参与Join 操作的两张表分别被称为**“流式表（ StreamTable ）”和“构建表（ BuildTable ）**”，不同表的角色在Spark SQL 中会通过一定的策略进行设定。通常来讲，**系统会默认将大表设定为流式表**，将小表设定为构建表。流式表的迭代器为streamedlter ，构建表的迭代器为buildlter 。**遍历streamedlter 中每条记录，然后在buildlter 中查找相匹配的记录。这个查找过程称为Build 过程。**每次Build 操作的结果为一条JoinedRow (A, B ），其中A 来自streamedlter, B 来自buildlter ，这个过程为BuildRight 操作；而如果B 来自streamedlter , A 来自buildlter ，则为BuildLeft 操作。

![1606547665305](.\images\1606547665305.png)

对于LeftOuter、RightOuter、LeftSemi 和LeftAnti ，它们的Build 类型是确定的，**即LeftOuter 、**
**LeftSemi 、LeftAnti 为BuildRight, RightOuter 为BuildLeft 类型**。对于Inner, BuildLeft 和BuildRight两种都可以，选择不同，可能有着很大的性能区别。

### BroadcastJoinExec 执行机制

**该Join 实现的主要思想是对小表进行广播操作，避免大量Shuffle 的产生**。Join 操作是对两个表中key 值相同的记录进行连接，在Spark SQL 中，**对两个表做Join 操作最直接的方式是先根据key 分区，然后在每个分区中把key 值相同的记录提取出来进行连接操作**。这种方式不可避免地涉及数据的Shuffle ，而Shuffle 在各个大数据计算框架中都是比较耗时的操作。因此，当一个大表和一个小表进行Join 操作时，为了避免数据的Shuffle ，可以将小表的全部数据分发到每个节点上，供大表直接使用。

在SparkSQL 中， BroadcastJoinExec 可以进行用户设置，触发BroadcastJoinExec 的场景有两个。

- 被广播的表需要小于参数（ spark.sql.autoBroadcastJoinThreshold ）所配置的值，默认是
  l0MB 。
- 在SQL 语句中人为地添加了Hint (**MAPJOIN 、BROADCASTJOIN 或BROADCAST** ） 。

**在Outer 类型的Join 中，基表不能被广播**，例如当A left outer join B 时，只能广播右表B 。**一般BroadcastJoinExec 只适用于广播较小的表，否则数据的冗余传输远大于Shuffle 的开销**。另外，**广播时需要将被广播的表读取到Driver 端**，当频繁有广播出现时，对Driver 端的内存也会造成较大压力。

BroadcastExchange 将广播表广播到每个节点上进行Join 操作：

![1606549359927](.\images\1606549359927.png)

### ShuffledHashJoinExec 执行机制

![1606549773988](.\images\1606549773988.png)

ShuffleHashJoinExec 执行机制分为两步:

- 对两张表分别按照Join key 进行重分区，即Shuffle ，目的是为了让有相同key 值的记录分到对应的分区中，这一步对应执行计划中的Exchange 节点。
- 对每个对应分区中的数据进行Join 操作，此处先将小表分区构造为一张Hash 表，然后根据大表分区中记录的key 值进行匹配，即执行计划中的ShuffledHashJoinExec 节点。

在介绍ShuffiedHashJoinExec 的实现机制之前，有必要先了解其父类HashJoin:

![1606550083888](.\images\1606550083888.png)

两个属性对（ buildPlan, streamedPlan ）与（ buildKeys, streamedKeys ），用来区分参与Join 的两个数据表（构建表和流式表）的角色。**构建表在Join 过程中会创建一个HashMap,用来支持数据的查找，属于“静态”的一方。流式表在Join 过程中， 一行一行地在构建表对应的HashMap 中查找数据，属于“动态”的一方。**

了解了HashJoin ，理解ShuffiedHashJoinExec 就比较简单了。ShuffiedHashJoinExec 是HashJoin
的子类，其执行过程的实现很简单，核心代码如下。**首先，对构建表建立HashedRelation ，然后**
**调用HashJoin 中的Join 方法，对当前流式表中的数据行在HashedRelation 中查找数据进行Join**
**操作。**

![1606550159637](.\images\1606550159637.png)

### SortMergeJoinExec 执行机制

在Spark SQL 中， SortMergeJoinExec 是Join 查询的主要实现方式。根据前面几个小节的分析可知， **Hash 系列的Join 实现中都将一侧的数据完全加载到内存中，这对于一定大小的表比较适用**。然而，当**两个表的数据量都非常大时，无论使用哪种方法都会对计算内存造成很大压力**。

![1606551171921](.\images\1606551171921.png)

根据其原理， SortMergeJoinExec 实现方式并不用将一侧数据全部加载后再进行Join 操作，**其前提条件是需要在Join 操作前将数据排序**。）

为了让两条记录能连接到一起，需要将具有相同key 的记录分发到同一个分区，因此一般会进行一次Shuffle 操作（物理执行计划中的Exchange 节点）。**根据Join 条件确定每条记录的key ，并且基于该key 进行分区**，将可能连接到一起的记录分发到同一个分区中，这样在后续的Shuffle 读阶段就可以将两个表中具有相同key 的记录分到同一个分区处理。

经过Exchange 节点操作之后，分别对两个表中每个分区里的数据按照key 进行排序（图8中SortExec 节点），然后在此基础上进行**merge sort** 操作。在遍历流式表时，**对于每条记录，都采用顺序查找的方式从构建查找表中查找对应的记录。由于排序的特性，每次处理完一条记录后只需要从上一次结束的位置开始继续查找。**

不同的Join 类型返回不同的RowIterator 作为Join 结果。Row Iterato r 主要实现了advanceNext 和getRow 两个方法，其中advanceNext 是将Iterator 向前移动一行，而getRow 用来获取当前行。在具体方法实现中， Row Iterat or 是通过调用对应的JoinScanner 接口来实现的。

![1606551784768](.\images\1606551784768.png)

SortMergeJoinScanner 是查找匹配数据的核心类，在SortMerge JoinScanner 的构造参数中会传递streamed Table 的迭代器（ streamedlter ）和bufferedTable 的迭代器（ bufferedlter),考虑到streamedTable 与bufferedTable 都是已经排好序的，**因此在匹配满足条件数据的过程中只需要不断移动迭代器， 得到新的数据行进行比较即可**。

![1606551855757](.\images\1606551855757.png)

具体细节可参考书籍《SparkSQL内核剖析》

