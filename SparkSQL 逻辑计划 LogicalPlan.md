---
typora-copy-images-to: images
---

## 逻辑计划转换过程

Spark SQL 逻辑计划在实现层面被定义为 LogicalPlan 。**SQL 语句经过 SparkSqlParser解析生成 Unresolved LogicalPlan** ，到最终优化成为 Optimized LogicalPlan ，这个流程主要经过3个阶段， 所示 3个阶段分别产生 Unresolved LogicalPlan, Analyzed LogicalPlan，Optimized LogicalPlan ，其中 Optimized LogicalPlan 传递到下 个阶段用于物理执行计划的生成。

![1606189890365](.\images\1606189890365.png)

具体来讲，这3个阶段所完成的工作分别如下：

1. **由 SparkSqlParser 中的 AstBuilder 执行节点访问，将语法树的各种 Context 节点转换成对应的 LogicalPlan 节点**，从而成为一棵未解析的逻辑算子树（Unresolved LogicalPlan ），此时的逻辑算子树是最初形态，不包含数据信息与列信息等。
2. **由 Analyzer 将一系列的Rule作用在 Unresolved LogicalPlan 上**，对树上的节点绑定各种数据信息，生成解析后的逻辑算子树（Analyzed LogicalPlan）。
3. **由 Spark SQL 中的优化器（ Optimizer ）将一系列优化Rule作用到上步生成的Analyzed LogicalPlan中**，在确保结果正确的前提下改写其中的低效结构，生成优化后的逻辑算子树（ Optimized LogicalPlan）

## AstBuilder 机制： Unresolved LogicalPlan 生成

SparkSession调用sessionState.sqlParser（ParserInterface接口类型）的parsePlan方法，将sqlText解析为Unresolved LogicalPlan。

![1606219501709](.\images\1606219501709.png)

parsePlan实际调用的是AbstractSqlParser中的方法。

![1606219671975](.\images\1606219671975.png)

parsePlan调用AbstractSqlParser中的parse方法。

![1606219750899](.\images\1606219750899.png)

parse方法中**首先利用ANTLR生成的词法解析器SqlBaseLexer、语法解析器SqlBaseParser对sqlText进行解析，获得抽象语法树**。然后调用toResult方法，即astBuilder.visitSingleStatement(parser.singleStatement()) 。**调用AstBuilder的visitSingleStatement方法，遍历访问抽象语法树，将其转换为LogicalPlan**。

![1606221576108](.\images\1606221576108.png)

![1606221599353](.\images\1606221599353.png)

visitSingleStatement是访问整个抽象语法树的启动接口。visitSingleStatement调用visit方法访问其子节点（ctx.statement ，默认为 StatementDefaultContext 节点）。

![1606221716692](.\images\1606221716692.png)

visit方法会调用当前树节点的accept方法，将访问者即astBuilder传入作为参数。

![1606221855162](.\images\1606221855162.png)

StatementDefaultContext中的accept会调用对应的astBuilder.visitStatementDefault方法。AstBuilder没有实现此方法，所以调用父类SqlBaseBaseVisitor中的visitStatementDefault方法，其又默认调用visitChildren方法，对当前节点的每个子结点调用accept方法，并且聚合获得到的节点，返回最后的聚合结果。

![1606222013065](.\images\1606222013065.png)

![1606222192663](.\images\1606222192663.png)

![1606222200160](.\images\1606222200160.png)

从上面的解析可以看出，**对根节点的访问操作会递归访问其子节点，这样逐步向下递归调用，直到访问某个子节点时能够构造 LogicalPlan ，然后传递给父节点，因此返回的结果可以转换为 LogicalPlan 类型**。当整个解析过程访问到 QuerySpecification Context 节点时，执行逻辑可以看作两部分：首先访问 FromClauseContext 子树，生成名为 from 的Logica Plan ；接下来，调用withQuerySpecification 方法在 from 的基础上完成后续扩展。

![1606222590771](.\images\1606222590771.png)

总的来看，生成 Unresolved Logica1Plan 的过程，从访问 QuerySpecificationContext
节点开始，分为以下3个步骤：

![1606222723645](.\images\1606222723645.png)

（1）生成数据表对应的 LogicalPlan：访问 FromC auseContext 并递归访问，一 直到匹配
TableNameContext 节点（visitTableName ）时，直接根据 TableNameContext 中的数据信息生成UnresolvedRelation ，此时不再继续递归访问子节点，构造名为 from的 LogicalPlan 并返回。

（2）生成加入了过滤逻辑的 LogicalPlan：：过滤逻辑对应 SQL 中的 where 语句，在 QuerySpecificationContext 中包含了名称为 where的 BooleanExpressionContext 类型，AstBuilder 会对该子树进行递归访问（例如碰到 ComparisonContext 节点时会生成 Greaterτhan 表达式），**生成 expression 并返回作为过滤条件**，然后基**于此过滤条件表**
**达式生成 Filter LogicalPlan 节点**，最后，由此 Logical Plan 和第（1 ）步中的 UnresolvedRelation构造名称为 withFilter LogicalPlan ，其中Filter节点为根节点。

（3）生成加入列剪裁后的 LogicalPlan：AstBuilder 在访问过程中会获取 QuerySpecificationContext节点所包含的 NamedExpressionSeqContext 成员，并对其所有子节点对应的表达式进行转换，生成NameExpression 列表（namedExpressions ）。然后，基于 namedExpressions 生成 Project LogicalPlan;最后，由 LogicalPlan 和第（2 ）步中的 withFilter 构造名称为 withProject LogicalPlan，其中Project最终成为整棵逻辑算子树的根节点。

### Expression生成

表5.1中访问操作按照子节点为先的顺序，当执行 visitColumnReference 时，会根据 ColumnReferenceContext 节点信息生成 UnresolvedAttribute 表达式， 其中的常数会统一封装为 Literal 表达式。在 visitPredicated中会检查该谓词逻辑中是否包含 predicate 语句（按照文法文件中的定义， predicate 主要表示BETWEEN-AND IN LIKE/RLIKE 等语句），这里的 SQL 不包含 predicate ，因此直接返回访问其子节点（visitComparison ）得到的结果 最终生成逻辑算子 Filter 节点的 condition 构造参数为GreaterThan 表达式。

![1606224678739](.\images\1606224678739.png)

表5.2访问操作按照子节点为先的顺序，当执行 visitColumnReference 时，会对 name 列生成 UnresolvedAttribute 表达式；此时 visitPredicated 中同样不包含 predicate ，因此直接返回子节点中得到的 Unresolve dAttribute;最后，执行 visitNamedExpression 访问操作，顾名思义，该操作用于对选取的列进行命名 ，因为语句中不涉及别名（Alias ）的情况，这里也是直接返回子节点生成的表达式。此外， SQL select 操作中因为只选取了 name 列，所以最终生成逻辑算子树 Project 节点的构造参数 即为Seq(UnresolvedAttribute(Seq（“NAME”）））表达式。

![1606224903739](.\images\1606224903739.png)

## Analyzer 机制： Analyzed LogicalPlan 生成

详细的解析过程可查看：[ SparkSQL Analyzed实例源码解析 ](https://blog.csdn.net/qq_41775852/article/details/105189828)

Analyzer继承至RuleExecutor， Analyzer 执行过程会调用其父类 RuleExecutor 中实现的
executor 方法。**Analyzer中重载了RuleExecutor中的batches方法，重新定义了一套规则**。

![1606273304648](.\images\1606273304648.png)



**在QueryExecution 类中可以看到，触发 Analyzer 执行的是 execute 方法，即 RuleExecutor中的execute 方法**，该方法会循环地调用规则对逻辑算子树进行分析。

![1606274266284](.\images\1606274266284.png)

execute方法将Analyzer定义所有的Rule应用于逻辑算子树。

![1606280168119](.\images\1606280168119.png)



比如ResolveRelations规则，利用Catalog将UnresolvedRelation解析为concrete relations。

![1606280398078](.\images\1606280398078.png)

apply方法调用LogicalPlan的resolveOperatorsUp方法。

![1606280450143](.\images\1606280450143.png)

resolveOperatorsUp方法利用其传入的偏函数（rule），以后序遍历、自下而上的方式，将偏函数（rule）应用于每个子节点，最后应用于自己，并且会跳过已经被analyzed的节点。

由于Rule使用了模式匹配的方法，所以只有匹配到对应的节点时，才会真正的进行解析操作。比如对于ResolveRelations规则，当遍历逻辑算子树的过程中匹配到 UnresolvedRelation 节点时，其会直接调用 lookupTableFromCatalog 方法从 Session Catalog查表。





![1606281931110](.\images\1606281931110.png)

![1606281943786](.\images\1606281943786.png)

![1606281953308](.\images\1606281953308.png)![1606281961236](.\images\1606281961236.png)

### Experssion analyzed

比如ResolveReferences规则与表达式解析有关。

![1606285308767](.\images\1606285308767.png)

同样是调用resolveOperatorsUp遍历逻辑算子树，对于LogicalPlan，其调用mapExpressions方法解析逻辑算子树中的表达式。

![1606285644807](.\images\1606285644807.png)

mapExpressions方法对LogicalPlan中的所有表达式调用传入的表达式转换方法，即resolve方法。

![1606285817725](.\images\1606285817725.png)

resolve方法中如果是UnresolvedAttribute，则调用resolveChildren方法对其进行解析。如果表达式不符合上述的情况，则调用Expression的mapChildren方法，对表达式树进行遍历解析。





## Optimizer机制：Optimized LogicalPlan生成

经过上一个阶段 Analyzer 的处理， Unresolved LogicalPlan 已经解析成为 Analyzed LogcalPlan。

Optimizer 同样继承自 RuleExecutor 类，本身没有重载RuleExecutor 中的 execute 方法，因此其执行过程仍然是调用其父类RuleExecutor 中实现的execute方法。 **QueryExecution 中， Optimizer 会对传入的 Analyzed LogicalPlan 执行 execute方法**，启动优化过程。

![1606283383218](.\images\1606283383218.png)

与Analyzer 类似， Optimizer 的主要机制也依赖 新定义的一系列规则，同样对应 RuleExecutor 类中的成员变量 batches。因此在 Rule Executor 执行 execute 方法时会直接利用这些规则Batch。

![1606283994041](.\images\1606283994041.png)

比如EliminateSubqueryAliases规则：

![1606284612967](.\images\1606284612967.png)

其调用LogicalPlan的transformUp方法：

![1606284777479](.\images\1606284777479.png)

以后序遍历、自底而上的遍历子节点，并将传入的偏函数（rule）应用于子节点，最后应用于当前节点。EliminateSubqueryAliases规则当遇到SubqueryAlias节点时，直接将当前节点转换为其子节点。

**总体过程和Analyze基本相似**。

![1606286133858](.\images\1606286133858.png)

![1606286142268](.\images\1606286142268.png)

![1606286173740](.\images\1606286173740.png)

经过上述步骤， SparkSQL 逻辑算子树生成、分析与优化的整个阶段都执行完毕 最终生成
的逻辑算子树包含 Relation 节点、 Filter 点和 Project 节点 ，**同时每个节点中又包含了由对应表达式构成的树**。