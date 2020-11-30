## Spark SQL执行过程

![](.\images\SQL执行过程.png)

逻辑计划阶段会将用户所写的 SQL 语句转换成树型数据结构（逻辑算子树）， SQL 语句中蕴含的逻辑映射到逻辑算子树的不同节点。 顾名思义，逻辑计划阶段生成的逻辑算子树并不会直接提交执行，仅作为中间阶段。最终逻辑算子树的生成过程经历3个子阶段，**分别对应**

1. 未解析的逻辑算子树（Unresoved LogicaPlan ，仅仅是数据结构，不包含任何数据信息等）
2. 解析后的逻辑算子树（Analyzed LogcalPlan ，节点中绑定各种信息）
3. 优化后的逻辑算子树（ Optimize LogcalPlan ，应用各种优化规则对些低效的逻辑计划进行转换）。

物理计划阶段将上 步逻辑计划阶段生成的逻辑算子树进行进一步转换，生成物理算子树
物理算子树的节点会直接生成 RDD 或对 RDD 进行 transformation 操作（注：每个物理计划节点中都实现了对 RDD 进行转换的 execute 方法） 同样地，物理计划阶段也包 含3个子阶段：

1. 首先，根据逻辑算子树，生成物理算子树的列表 Iterator[Physican] （同样的逻辑算子树可能对应多个物理算子树）；
2. 然后，从列表中按照 定的策略选取最优的物理算子树（SparkPlan ）；
3. 最后，对选取的物理算子树进行提交前的准备工作，例如，确保分区操作正确、物理算子树节点重用、执行代码生成等，得到“准备后”的物理算子树（Prepared SparkPlan）。

经过上述步骤后，，物理算子树生成的 RDD 执行 action 操作（如例子中的 show ），即可提交执行。

**从SQL 语句的解析一直到提交之前，上述整个转换过 Spark 集群 Driver 端进行**，不涉及分布式环境 SparkSession 类的 sql方法调用 SessionState 中的 对象 ，**包括上述不同阶段对应的 SparkSqlParser 类、 Analyzer 类、 Optimizer，SparkPlan 类**等，最后封装成Query Execution 对象。



