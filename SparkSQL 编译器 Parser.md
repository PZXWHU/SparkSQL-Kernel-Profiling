---
typora-copy-images-to: images
---

## DSL 工具之 ANTLR 简介

DSL（Domain Specific Language） 的构建与通用编程语言的构建类似，主要的过程仍然是指定语法和语义，然后实现编译器或解释器 通常情况下，一个系统中 DSL 模块的实现需要涉及两方面的工作。

- 设计语法和语义，定义 DSL 中具体的元素
- 实现**词法分析器**（Lexer ）和**语法分析器**（Parser ），完成对 DSL 的解析，最终转换为底层逻辑来执行

经过几十年的研究，编译理论已经比较成熟，借助于各种工具，开发人员不再需要从头开始构建烦琐的词法分析和语法分析模块 迄今为止，业界提供了各种各样的生成器（Generator) 。

ANTLR (Another Tool for Language Recognition ）是目前非常活跃的语法生成工具，用 Java语言编写，基于 **LL （＊）解析方式** ，使用自上而下的递归下降分析方法。 **ANTLR 可以用来产生词法分析器、语法分析器和树状分析器（Tree Parser ）等各个模块，其文法定义使用类似EBNF (Extended Backus-Naur Form ）的方式**，简洁直观。ANTLR4 除能够自动构建语法分析树外，还支持生成基于监听器（Listener ）模式和访问者(Visitor ）模式的树遍历器。

## ANTLR 计算器实例

ANTLR4 中，词法和语法可以放在同 G4 文件中，词法单元以大写字母开头，语法单元以小写字母开头。

```
grammar Calculator;

line : expr EOF ;
expr : '(' expr ')'          # parseExpr
    | expr ('*' | '/') expr  # multOrDiv
    | expr ('+' | '-') expr  # addOrSubtact
    | FLOAT                  # float ;

WS : [ \t\n\r]+ -> skip;
FLOAT : DIGIT + '.' DIGIT* EXPONENT ?
    | '.' DIGIT + EXPONENT ?
    | DIGIT + EXPONENT ? ;

fragment DIGIT : '0' .. '9';
fragment EXPONENT : ('e' | 'E')('+' | '-') ? DIGIT+ ;
```

在上述文法文件中，“grammer Calculator"表示文件是 个词法、语法混合文件，名称必须和文件名相同，即该文法文件的文件名应该是 Calculator.g4。需要注意的是， expr 每条规则后面的“＃”是产生式标签名（Alternative Label Name ），起到标记不同规则的作用。词法规则 ws 定义了空格，其中的“－＞ skip ”是 ANTLR4 中特殊的命令，表示直接跳过不做任何处理。基于此G4文件可使用ANTLR生成对应代码：

![1606120284527](.\images\1606120284527.png)

其中 Calculator. tokens，CalculatorLexer. tokens 是内部的 Token 定义， CalculatorLexer，Calculator Parser 是生成的词法分析器和语法分析器，剩下的 Java 文件代表着两种访问语法树的方式， CalculatorListener，CalculatorBaseListener 对应监昕器模式， CalculatorVisitor ，CalculatorBaseVisitor 对应访问者模式。CalculatorBaseListener和CalculatorBaseVisitor 是 CalculatorListener和CalculatorVisitor 默认实现（**BaseVisitor中的所有方法都是调用visitChildren(ctx)方法，ctx为当前语法解析树节点**）。

**visitChildren(ctx)的实现：将访问者应用于所有子节点，并聚合获得到的子节点的结果。**

```java
public T visitChildren(RuleNode node) {
        T result = this.defaultResult();
        int n = node.getChildCount();

        for(int i = 0; i < n && this.shouldVisitNextChild(node, result); ++i) {
            ParseTree c = node.getChild(i);
            T childResult = c.accept(this);
            result = this.aggregateResult(result, childResult);
        }

        return result;
    }

protected T aggregateResult(T aggregate, T nextResult) {
        return nextResult;
    }
```





使用访问者模式，扩展CalculatorBaseVisitor类。**Visitor类实现类中对于每个节点的访问访问一般都是，先递归访问其子节点，利用获取到的子节点的访问结果生成当前节点的结果。**

```java

import com.pzx.antlr.CalculatorBaseVisitor;
import com.pzx.antlr.CalculatorParser;

public class MyCalculatorVisitor extends CalculatorBaseVisitor {

    @Override
    public Object visitMultOrDiv(CalculatorParser.MultOrDivContext ctx) {

        Object o1 =  ctx.expr(0).accept(this);
        Object o2 =  ctx.expr(1).accept(this);

        if("*".equals(ctx.getChild(1).getText())){
            return (Float)o1 * (Float)o2;
        }else{
            return (Float)o1 / (Float)o2;
        }
    }

    @Override
    public Object visitAddOrSubtact(CalculatorParser.AddOrSubtactContext ctx) {

        Object o1 =  ctx.expr(0).accept(this);
        Object o2 =  ctx.expr(1).accept(this);

        if("+".equals(ctx.getChild(1).getText())){
            return (Float)o1 + (Float)o2;
        }else{
            return (Float)o1 - (Float)o2;
        }
    }

    @Override
    public Object visitFloat(CalculatorParser.FloatContext ctx) {
        return Float.parseFloat(ctx.getText());
    }

    @Override
    public Object visitParseExpr(CalculatorParser.ParseExprContext ctx) {
        Object o = ctx.expr().accept(this);
        return o;
    }
}

```

驱动程序如下，根据输入的字符流相继构造词法分析器（Lexer ）和语法分析器（Parser ），然后 建相应Visitor 来访问语法分析器解析得到的语法树， 最后返回结果。

```java
public class Calculator {

    public static void main(String[] args) {
        String query = "3.1*(6.3-4.51)";
        CalculatorLexer calculatorLexer = new CalculatorLexer(new ANTLRInputStream(query));
        CalculatorParser calculatorParser = new CalculatorParser(new CommonTokenStream(calculatorLexer));
        CalculatorBaseVisitor calculatorBaseVisitor = new MyCalculatorVisitor();
        System.out.println(calculatorBaseVisitor.visit(calculatorParser.expr()));
    }

}
```



## SparkSqlParser之 AstBuilder

Catalyst 中提供了直接面向用户的 Parselnterface 接口，该接口中包含了对SQL 语句、Expression 表达式和 Table Identifier 数据表标识符的解析方法 AbstractSqlParser 是实现了 Parselnterface 的虚类，其中定义了返回 AstBuilder 的函数。

AstBuilder （**遵循后序遍历方式，首先会生成子节点的 LogicalPlan ，然后生成当前节点的 LogicalPlan**），它继承了 ANTLR4 生成的默认SqlBaseBaseVisitor ，用于生成 SQL 对应的抽象语法树 AST (Unresolved LogicalPlan); SparkSqlAstBuilder 继承 AstBuilder ，并在其基础上定义了 DDL 语句的访问操作，主要在 SparkSqlParser中调用。

![1606120666145](.\images\1606120666145.png)

**所以SparkSQL首先根据定义的ANTLR文件生成了默认实现的访问者SqlBaseBaseVisitor ，然后自己创建AstBuilder、SparkSqlAstBuilder继承SqlBaseBaseVisitor实现自己解析语法树转换为逻辑计划的逻辑。**

**SparkSQL定义了ParseInterface接口，定义了解析SQLText转换为Expression或逻辑计划的方法，并创建AbstractSparkSqlParser、SparkSqlParser类继承ParseInterface接口，并组合AstBuilder、SparkSqlAstBuilder类，利用其遍历解析语法树的能力，实现自己的将SQL转换为Expression或者逻辑计划的方法。**

## 抽象语法树生成及遍历过程

```sql
select name from student where age > 18 order by id desc
```

![1606123542245](.\images\1606123542245.png)

AstBuilder调用visitSingleStatement访问根节点SingleStatementContext，采用后序遍历的方式，先生成子节点的LogicalPlan，然后利用子节点的LogicalPlan和当前节点的信息，生成当前的LogicalPlan，返回结果，即完成抽象语法树的遍历转换工作