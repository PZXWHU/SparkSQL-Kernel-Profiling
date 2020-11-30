---
typora-copy-images-to: images
---

## Aggregation 执行概述

### 文法定义



在Catalyst 的SqlBase.g4 文法文件中，聚合语句 aggregation 定义如下：在常见的聚合查询中，通常包括**分组语句（group by ）和聚合函数（ aggregate function ）**；聚合函数出现在 Select句中的情形较多，定义在 function Call 标签的 primaryExpression 表达式文法中， qualifiedName对应函数名，括号内部是该函数的参数列表。

![1606372351507](.\images\1606372351507.png)

举例：select id, count(name) from student group by id ;

![1606372565868](.\images\1606372565868.png)

加入聚合操作后的语法树最重要的元素是 FunctionCall Context 节点和 AggregationContext节点。Aggregation Context 节点反映在语法树中即图  QuerySpecificationContext 节点下的第3个子节点，**其子节点（从ExpressionContext 一直到 ColwnnReferenceContext ）对应 group by句后面的 id**。用来表示聚合函数的 FunctionCall Context 节点的结构比较好理解，其子节点**QualifiedNameContext 代表函数名， Expression Context 表示函数的参数表达式（对应 SQL 语句中name 列）**

### 聚合语句 Unresolved LogicalPlan 生成

在SparkSQL 中， Aggregate 逻辑算子树节点是 UnaryNode 中的一种，属于基本的逻辑算子(basicLogicalOperator）。**该逻辑算子树节点通过分组表达式列表（groupingExpressions ）、聚合表达式列表（ aggregateExpressions ）和子节点（child ）构造而成**，其中分组表达式类型都是 Expression ，而聚合表达式类型都是 NamedExpression ，意味着聚合表达式一般都需要设置名字。同时， Aggregate 的输出函数 output 对应聚合表达式列表中的所有属性值。

![1606373264922](.\images\1606373264922.png)

上述聚合查询从抽象语法树生成 Unresolved LogicalPlan 主要涉及以下函数的调用。

- 针对 QuerySpecificationContext 节点，执行 visitQuerySpecification ，会先后调用 visitFromClause 和withQuerySpecification 函数。
- 在 visitFromClause 数中 ，针对 FromClauseContext 节点生成 UnresolvedRelation 逻辑算子节点，对应数据表。
- 在返回的 UnresolvedRelation 节点上，执行 withQuerySpecification 函数，实际上这里具体执行的是 **withAggregation 函数**，**在 UnresolveRelation 节点上生成 Aggregate 逻辑算子树节点**，返回完整的逻辑算子树。

![1606373517923](.\images\1606373517923.png)

visitFunctionCall ：visitFunctionCall 法由 visitNamedExpression 调用， 属于 Select 语句中的一部分，用来生成聚合函数对应的表达式。最终得到的 UnresolvedFunction 包含了聚合函数名 、参数列表和是否
包含 distinct 的布尔值。

### 从逻辑算子树到物理算子树

上述聚合语句同样涉及 Analyzed LogicalPlan、 Optimized LogicalPlan、 SparkPlan、 ExecutedPlan 多个步骤。

![1606373688163](.\images\1606373688163.png)

从Unresolved LogicalPlan 到Analyzed LogicalPlan 经过了4条规则的处理。对于聚合查询来说，比较重要 是其中的 ResolveFunctions 规则，用来分析聚合函数。对于 UnresolvedFunction表达式， Analyzer 会根据函数名和函数参数去 Session Catalog 中查找，而 Session Catalog 会根据FunctionRegistry 中已经注册的函数信息得到对应的聚合函数（AggregateFunction）。

从Analyzed LogicalPlan到Optimized LogicalPlan 分别经过了别名消除（EliminateSubqueryAliases ）规则与列剪裁（ ColumnPruning 规则的处理，这里不再赘述。

而从 Optimized LogcialPlan到物理执行计划 SparkPlan 进行转换时，主要经过了 FileSourceStrategy Aggregation 这两个策略（Strategy ）的处理。 FileSourceStrategy 会应用到 Project和Relation 节点。对于聚合查询，逻辑算子树转换为物理算子树，必不可少的是 Aggregation 转换策略。实际上， Aggregation 策略是基于 PhysicalAggregation，其是一种逻辑算子树的模式，用来匹配逻辑算子树中的 Aggregate 节点并提
取该节点中的相关信息 PhysicalAggregation 在提取信息时会进行以下转换。

![1606374660958](.\images\1606374660958.png)

得到上述各种聚合信息之后， Aggregation 策略会根据这些信息生成相应的物理计划。当聚合表达式中存在不支持 Partial 方式且不包含 Distinct 函数时，调用的是 planAggregateWithoutPartial 方法；当聚合表达式都支持 Partial方式且不包含 Distinct 函数时，调用的是 planAggregateWithoutDistinct 方法；当聚合表达式都支持Partial 方式且存在 Distinct 函数时，调用的是 planAggregateWith OneDistinct 方法。

![1606377041544](.\images\1606377041544.png)

![1606374727657](.\images\1606374727657.png)

因为 count 函数支持 Partial 方式，因此调用的是planAggregate WithoutDistinct 方法，生成了图 中的两个 HashAggregate （聚合执行方式中的一种）物理算子树节点，分别进行局部聚合与最终的聚合。最后，在生成的
SparkPlan 中添加 Exchange 节点，统一排序与分区信息，生成物理执行计划（ExecutedPlan）。

## 聚合函数

聚合函数（AggregateFunction） 是聚合查询中非常重要的元素。在实现上，**聚合函数是表达式中的一种**，和 Catalyste 中定义的聚合表达式（AggregationExpression ）紧密关联。**无论是在逻辑算子树还是物理算子树中，聚合函数都是以聚合表达式的形式进行封装的**，同时聚合函数表达式中也定义了直接生成聚合表达式的方法。

聚合表达式（AggregationExpression ）的成员变量和函数如图，resultAttribute 表示聚合结果，获取子节点的 children 方法并返回聚合函数表达式； data Type 函数直接调用聚合函数中的 dataType 函数获取数据类型。

![1606375571990](.\images\1606375571990.png)

### 聚合缓冲区与聚合模式（AggregateMode) 

**1. 聚合函数缓冲区**

聚合函数缓冲区是指在同一 个分组的数据聚合的过程中，用来保存聚合函数计算中间结果的内存空间。

聚合函数缓冲区的定义有一个前提条件，即**聚合函数缓冲区针对的是处于同一个分组内（实例中属于同一 id ）的数据**。需要注意的是，查询中可能包含多个聚合函数，因此**聚合函数缓冲区是多个聚合函数所共享的**。

在聚合函数的定义中，与聚合缓冲区相关的基本信息包括：聚合缓冲区的 Schema 信息( aggBufferSchema ），返回为 StructType 类型；聚合缓冲区的数据列信息（aggButfer Attributes ），返回的是 AttributeReference 列表（Seq [ AttributeReference ）），对应缓冲区数据列名。

**2. 聚合模式**

**在SparkSQL 中，聚合过程有4种模式，分别是 Partial 模式、 ParitialMerge 模式、 Final 模式、Complete 模式。**

**Final 模式一般和 Partial 模式组合在一起使用**。**Partial 模式可以看作是局部数据的聚合**，在具体实现中， Partial 模式的聚合函数在执行时会根据读入的原始数据更新对应的聚合缓冲区，当处理完所有的输入数据后，返回的是聚合缓冲区中的中间数据。 **Final 模式所起到的作用是将聚合缓冲区的数据进行合并**，然后返回最终的结果 。在最终分组计算总和之前，可以先进行局部聚合处理，这样能够避免数据传输并减少计算量。因此，上述聚合过程中**在 map 阶段的 sum 函数处于 Partial 模式，在 reduce 阶段的 sum 函数处于 Final 模式**。

![1606376111144](.\images\1606376111144.png)

**Complete 模式和上述的 Partial/Final 组合方式不一样，不进行局部聚合计算**。一般来讲， Complete 模式应用在不支持 Partial 模式的聚合函数中。

![1606376169671](.\images\1606376169671.png)

**PartialMerge 模式的聚合函数主要是对聚合缓冲区进行合并，**但此时仍然不是最终的结果， **ParitialMerge 主要应用在 distinct 语句中**。

![1606376391927](.\images\1606376391927.png)

### DeclarativeAggregate 聚合函数

DeclarativeAggreg由聚合函数是一类直接由 Catalyst 中的表达式（Expressions ）构建的聚合函数,主要逻辑通过调用4个表达式完成，分别是 initialValues （聚合缓冲区初始化表达式）、updateExpressions （聚合缓冲区更新表达式）、 mergeExpressions （聚合缓冲区合并表达式）和evaluateExpression （最终结果生成表达式）。

![1606376714639](.\images\1606376714639.png)

通常来讲，实现一个基于表达式的 DeclarativeAggregate 函数包含以下几个重要的组成部分：

- 定义一个或多个聚合缓冲区的聚合属性（bufferAttribute ），例如 count 函数中只需要 count,这些属性会在 updateExpressions 等各种表达式中用到。
- 设定 DeclarativeAggregate 函数的初始值， count 函数的初始值为0.
- 实现数据处理逻辑表达式 updateExpressions ，在 count 函数中， 当处理新的数据时，上述定义的 count 属性转换为 Add 表达式，即 count+ lL ，注意其中对 Null 的处理逻辑。
- 实现 merge 处理逻辑的表达式，函数中直接把 count 相加，对应上述代码中的“count.left＋count.right”，由 DeclarativeAggregate 中定义的 RichAttribt出隐式类完成。
- 实现结果输出的表达式 evaluateExpression ，返回 count值。

### lmperativeAggregate 聚合函数

不同于 DeclarativeAggregate 聚合函数基于 Catalyst 表达式的实现方式， ImperativeAggregate聚合函数需要显式地实现 initialize、 update 、merge 方法来操作聚合缓冲区中的数据。一个比较显著的不同是， ImperativeAggregate 聚合函数所处理的聚合缓冲区本质上是基于行（InternalRow类型）的。

![1606377208271](.\images\1606377208271.png)

### TypedlmperativeAggregate 聚合函数

TypedimperativeAggregate [T] 聚合函数允许使用用户自定义的 Java 对象 作为内部的聚合，因此这种类型的聚合函数是最灵活。**TypedimperativeAggregate聚合缓冲区容易带来与内存相关的问题**。

![1606377653205](.\images\1606377653205.png)

## 聚合执行

### 聚合方式

聚合执行本质上是将 RDD 的每个 Partition 中的数据进行处理 。对于每个Partition 中的输入数据即 Input （通过 Inputlterator 进行读取），经过聚合执行计算之后，得到相应的结果数据即 Result （通过 Aggregationlterator 来访问）。

![1606377737109](.\images\1606377737109.png)

聚合查询的最终执行有两种方式：**基于排序的聚合执行方式（SortAggregateExec ）与基于 Hash 的聚合执行方式（HashAggregateExec）。**常见的聚合查询语句通常采用 HashAggregate 方式，当存在以下几种情况时，会用 SortAggregate 方式来执行：

- **查询中存在不支持 Partial 方式的聚合函数**：此时会调用 AggUtils 中的 planAggregateWithoutPartial 方法，直接生成 SortAggregateExec 聚合算子节点。
- **聚合函数结果不支持 Buffer 方式**：如果结果类型不属于（NullType, BooleanType, ByteType, ShortType, IntegerType, LongType, FloatType, DoubleType, DateType, TimestampType,Decimal Type］集合中的任意 一种，则需要执行 SortAggregateExec 方式。例如 collect_set和collect_list。
- **内存不足**：如果在 HashAggregate 执行过程中，内存空间己捕，那么聚合执行会切换到SortAggregateExec 方式。

### 执行框架 Aggregationlterator

**聚合执行框架指的是聚合过程中抽象出来的通用功能，包括聚合函数的初始化、聚合缓冲区更新合并函数和聚合结果生成函数等** 。这些功能都在聚合迭代器（Aggregationlterator ）中得到了实现。

聚合迭代器定义了3个重要的功能，分别是**聚合函数初始化**（initialize AggregateFunctions ）、**数据处理函数生成**（generateProcessRow ）和**聚合结果输出函数生成**（generateResultProjection)。

- 聚合函数初始化，可以细分为两个阶段：第一阶段：funcWithBoundReference 执行 bindReference或设置聚合缓冲区偏移量 针对 Partial、 Complete 模式的 ImperativeAggreg耐聚合函数，AttributeReference 表达式会转换为 BoundReference 表达式。而对于 Partia Merge和Final 模式的 ImperativeAggregate 聚合函数，会设置输入缓冲区的偏移 withNewlnputAggBufferOffset。第二阶段，funcWithUpdatedAggBufferOffset设置 ImperativeAggregate 函数聚合缓冲区的偏移量（withNewMutableAggBufferOffset ）。

  ![1606379796992](.\images\1606379796992.png)

- **数据处理函数 processRow** ，其参数类型是 (InternalRow、InternalRow），分别代表当前的聚合缓冲区 currentBufferRow 和输入数据行 row ，输出是 Unit型。**数据处理函数 processRow 的核心操作是获取各 Aggregation 中的 update 函数或 merge 函数**。对于 Partial Complete 模式，处理的是原始输入数据，因此采用的是 update 函数；而对于 Final PartialMerge 模式，处理的是聚合缓冲区，因此采用的是 merge 函数。

  ![1606379809256](.\images\1606379809256.png)

- **聚合结果输出函数**，输入类型是（UnsafeRow, InternalRow），输出的是 UnsafeRow 数据行类型。对于 Partial PartialMerge 模式的聚合函数，因为只是中间结果，所以需要保存 grouping 语句与 buffer 中所有的属性；对于 Final、Complete 聚合模式，直接对应 resultExpressions 表达式 。特别注意，如果不包含任何聚合函数且只有分组操作，则直接创建 projection。

  ![1606379955377](.\images\1606379955377.png)

### 基于排序的聚合算子 SortAggregateExec

这是一种基于排序的聚合实现，在进行聚合之前，会根据 grouping key 进行分区并在分区 内排序，**将具有相同**
**grouping key 的记录分布在同一个 partition 内且前后相邻**。聚合时只需要顺序遍历整个分区内的数据，即可得到聚合结果。

通过查看 SortAggregateExec 实现可知， requiredChild Ordering 中对输入数据的有序性做了约束，分组表达式列表（groupingExpressions ）中的每个表达式e都必须满足升序排列，即SortOrder（e， Ascending），**因此在 SortAggregateExec 节点之前通常都会添加一个 SortExec 节点**。**SortExec只会进行本地分区排序**，所以如果想要进行全局排序，则需要先SortExec加入RangeParition类型的Exchange节点。

![1606381333345](.\images\1606381333345.png)

SortBasedAggregationlterator 是SortAggregateExec 实现的关键，由于数据已经预先排好序，因此实现相对简单，按照分组进行聚合即可。分组数据处理算法逻辑如 Algorithm 所示，由算法可知 ，在 while 循环过程中，不断获取输入数据并将其赋值给 currentRow ，执行 groupingProjection 得到 groupingKey 分组表达式，如果当前的分组表达式 currentGroupingKey和groupingKey 相同，那么意味着当前输入数据仍然属于同 一个分组内部，因此调用 Aggregationlterator 中得到的 processRow 函数来处理当前数据。

![1606381102024](.\images\1606381102024.png)

### 基于 Hash 的聚合算子 HashAggregateExec

HashAggregateExec 从逻辑上很好实现，只要 建一个 Map 类型的数据结构，以分组的属性作为 key ，将数据保存到该 Map 并进行聚合计算即可。在实际系统中，无法确定性地申请到足够的空间来容纳所有数据 ，底层还涉及复杂的内存管理。**在实际系统中，无法确定性地能否申请到足够的空间来容纳所有数据 ，底层还涉及复杂的内存 理，因此相对 SortAggregateExec的实现方式反而更加复杂**。

观察 HashAggregateExec 的实现， requiredChildDistribution 对输入数据的分布做了约束，如果存在分区表达式，那么数据分布必须是 ClusteredDistribution 类型。**HashAggregateExec 的实现关键在于TungstenAggregationlterator 类**

![1606381489839](.\images\1606381489839.png)

![1606381590594](.\images\1606381590594.png)

