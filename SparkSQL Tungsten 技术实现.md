---
typora-copy-images-to: images
---

## 概述

Tungsten 是开源社区专门用于提升Spark 性能的计划。因为Spark 是用Scala 语言开发的，所以最终**运行在JVM 上的代码存在着各方面的限制和弊端（例如GC 上的overhead ）**，使得Spark在性能上有很大的提升空间。基于此考虑， Tungsten 计划应运而生，**旨在从内存和CPU 层面对Spark 的性能进行优化**。

## 内存管理和二进制处理

Tungsten 设计了一套内存管理机制， **Spark 可以直接操作二进制数据而不是JVM 对象**，使得内存使用效率大幅提升。

### Spark内存管理基础

Spark 为数据存储和计算执行提供了统一的内存管理接口（ MemoryManager ） 。在具体实现上，在Driver 端创建Memory Manager 的过程如以下代码所示

![1606622197975](.\images\1606622197975.png)

在Spark 1.6 版本之前， Spark 采用的是静态内存管理（ StaticMemoryManager ）方式。从1.6版本开始， Spark 默认采用统一内存管理（ UnifiedMemoryManager ）方式，但静态内存管理方式仍然保留（通过spark.memor川seLegacyMode 参数配置） 。相比静态内存管理方式，统一内存管理方式中存储内存和计算内存能够共享同一个空间，并且可以动态地使用彼此的空闲区域。

#### Spark 内存管理基本概念与内存管理器

##### 内存模式（MemoryMode)

MemoryManager 将内存模式分为堆内（ ON_HEAP ）内存和堆外（ OFF_HEAP)内存。在实际Spark 应用中用户经常使用参数（ spark.driver.memory和spark.executor.memory ）来配置Driver 与Executor 内存的大小。**这里配置的内存就是指堆内内存**。同一个Executor 中并发运行的多个Task 会共享Executor 进程的JVM 内存。堆内内存的申请与释放在本质上都是由JVM 完成的，**因此Spark 对堆内内存的管理只是在逻辑上进行记录和规划。**例如， Spark 在创建对象实例时，由JVM 分配内存空间并返回该对象的引用， Spark 负责保存该引用并记录该对象占用的内存。同样的，释放内存也由JVM的垃圾回收机制完成。

Spark 中序列化的对象是字节流的形式，其占用的内存大小可直接计算，**而对于非序列化的对象，其占用的内存是通过周期性地采样近似估算而得的**，并不是每次新增的数据项都会计算一次占用的内存大小，这种方法降低了时间开销，但是有可能误差较大，导致某一时刻的实际内存有可能远远超出预期。此外，被Spark 标记为释放的对象实例，实际上很有可能并没有被JVM 回收，导致实际可用的内存大小小于Spark 记录的可用内存大小。**所以， Spark 并不能准确记录实际可用的堆内内存，也就无法完全避免内存溢出（ Out of Memory)的异常。**

虽然不能精准控制堆内内存的申请和释放，但**Spark 通过对存储内存和执行内存各自独立的规划管理**，可以决定是否要在存储内存里缓存新的RDD ，以及是否为新的任务分配执行内存，在一定程度上可以提升内存的利用率，减少异常的出现。

JVM 堆外内存管理的引入使得Spark 可以很方便地直接在工作节点系统内存中分配空间，能够进一步优化内存的使用，减少了不必要的内存开销（例如频繁的垃圾回收），提升了处理性能。**堆外内存可以被精确地申请和释放，而且序列化的数据占用的空间可以被精确计算**。相对来讲，堆外内存比堆内内存更加容易管理。**在默认情况下堆外内存并不启用，可通过参数( spark.memory.offHeap.enabled ）开启，同样也可以通过参数（ spark.memory.offHeap.size ）控制堆外内存空间的大小。**

##### 内存池（MemoryPool) 

MemoryManager 通过内存池机制管理内存。内存池就是内存空间中一段大小可以调节的区域。在具体实现上， MemoryPool 是一个抽象类，内部定义了一个用来表示内存池大小的“_poolSize”变量和5 个相关的函数。

![1606622901370](.\images\1606622901370.png)

在Spark 中， MemoryPool 抽象类的具体实现有**StorageMemoryPool 和ExecutionMemoryPool** 两种，分别对应**数据存储的内存池和计算执行的内存池**。由于存在不同的**内存模式(MemoryMode)** , MemoryManager 实际上涉及4个资源池。数据存储内存池包含onHeapStorageMemoryPool（堆内）和offHeapStorageMemoryPool （堆外）两个，计算执行内存池包含onHeapExecutionMemoryPool （堆内）和offHeapExecutionMemoryPool （堆外）两个。

##### 内存页（page)

MemoryManager 中设定了内存页的大小（ pageSizeBytes ），单位为字节。如果用户没有显示地通过配置参数（ spark. buffer. pageSize ）设置该值，则在MemoryManager 中会采用以下公式来计算，从而得到默认值。

![1606664776283](.\images\1606664776283.png)

根据公式可知，最小的内存页大小为lMB ，最大的内存页大小为64MB 。

##### 内存管理接口（MemoryManager ）

MemoryManager 提供了3 个方面的内存管理接口包括**内存申请接口、内存释放接口和获取内存使用情况**的接口。在Spark 2.1 版本中，内存申请接口又包含存储内存申请（ acquireStorageMemory）、展开内存申请（ acquireUnrollMemory）和执行内存申请（ acquireExecutionMemory ） 。这里需要注意的是， **Spark 在将RDD 中的数据分区由不连续的存储空间组织为连续存储空间的过程被称为“展开”（ Unroll ），展开内存即用于这个操作。**

![1606664890419](.\images\1606664890419.png)

##### 静态内存管理（StaticMemoryManager）

StaticMemoryManager 是1.6 版本之前唯一的内存管理器，当前版本中都默认设置为关闭状态。**其是MemoryManager的子类**，主要重载实现了抽象类中的acquireStorageMemory、acquireUnrollMemory 和acquireExecutionMemory 3 个内存申请方法。将其称为静态内存管理的主要原因在于**数据存储与计算执行的内存占比和界限是固定的，彼此不能互相占用内存**。需要注意的是，**静态内存管理器中是不支持堆外内存作为数据存储的，因此实际上只有3 个内存池，**并且在申请数据存储内存时（ acquireStorageMemory ），内存模式（MemoryMode ） 应该是堆内模式（ ON_HEAP ） 。

MemoryManager 会将内存分为存储内存、执行内存和其他3 部分，**数据存储内存的比例通过参数spark.storage.memoryFraction 设置，默认比例为0.6, Unroll部分内存也属于存储内存的一部分；计算执行内存的比例通过参数spark.shuffle.memoryFraction设置，默认比例为0.2; 剩下的部分内存被系统保留，通常用来存储运行中产生的一些对象。**

![1606665190569](.\images\1606665190569.png)

通过上述比例得到的数据存储内存和计算执行内存一般不会被完全使用，为了防止内存溢出，**都会按照一定比例设置安全的使用空间**。计算执行内存部分的安全比例通过参数spark.shuffle. safetyFraction 设置，默认为0.8 ；数据存储内存部分的安全比例通过参数spark.storage.safetyFraction 设置，默认为0.9, Unroll 内存处于安全空间中，同样可以通过参数spark.storage.unrol!Fraction 设置，默认比例为0.2。

举例：例如，如果JVM 的内存共100GB ， 则在默认参数设置下，系统保留100 × 0.2 = 20GB ；计算执行内存共100 × 0.2 = 20GB ，其中安全空间为20 × 0.8 = 16GB ，保留4GB ；数据存储内存共100 × 0.6 = 60GB ，其中保留60 x 0.l = 6GB ，安全空间为60 × 0 .9 = 54GB ，在安全空间中， Unroll 内存为54 × 0 . 2 = 10.8GB 。

##### 统一内存管理机制（UnifiedMemoryManager）

UnifiedMemoryManager 同样也是MemoryManager 的子类，重载实现了
acquireStorageMemory、acquireUnrollMemory 和acquireExecutionMemory 3 个内存申请的方法。需要注意的是， **UnifiedMemoryManager 中不再单独管理Unroll 内存的申请，而是统一到数据存储内存的管理中。**

UnifiedMemoryManager 的内存分配机制如图所示，**比较明显的是其中多了一个300MB**
**的固定保留的内存空问（ Reserved ），这部分内存用户无法使用且大小不允许修改， 一般用于**
**Spark 系统内部使用**。除这部分内存外，剩下的称为**可用内存（ usableMemory）**，可用内存中除留给应用的内存外（例如存储用户程序中的数据结构），就是数据存储与计算执行公用的内存
区域。**数据存储与计算执行公用的内存内存的大小通过参数spark.memoryFraction 设置，默认为0.6** （注：设计文档中初始实现为0.75 ，后来因为考虑到GC ，在2.0 版本中进行了调整） 。此外，**数据存储内存空间大小的初始比例也可以通过参数spark.memory. storageFraction 设置，默认为0.5** ，即各占一半。

![1606665703310](.\images\1606665703310.png)

举例：假设JVM 内存共有1300MB ，那么最初状态下，数据存储和计算执行内存池所拥有的内存均为（ 1300-300 ） × 0.6 × 0.5=300MB 。随着应用的执行，**数据存储与计算执行间互相借用内存**，可能会导致这个数值不断变化。

统一内存管理最大的特点在于动态占用机制，其规则如下：

- 设定基本的存储内存和执行内存区域（spark.memory.storageFraction 参数），该设定确定了双方各自拥有的空间的范围
- 双方的空间都不足时，则存储到硬盘；若己方空间不足而对方空余时，可借用对方的空间;（存储空间不足是指不足以放下一个完整的 Block）
- **执行内存的空间被存储内存占用后，可让存储内存将占用的部分转存到硬盘，然后”归还”借用的空间**
- **存储内存的空间被执行内存占用后，无法让执行内存”归还”**，因为需要考虑 Shuffle 过程中的很多因素，实现起来较为复杂

#### 存储内存管理

**存储内存管理的入口是StorageMemoryPool**，StorageMemoryPool 是MemoryPool的子类，构造参数是**用于加锁的对象（ Lock ）和内存模式（ MemoryMode ）** 。StorageMemoryPool内部包括poolName 、memory Used 和＿memoryStore 3 个变量。其中， poolName 是内存池的名称（ on-heap execution 或off-heap execution); _memor yUsed 用于记录当前存储内存池的内存使用量；＿ memoryStore 是**MemoryStore** 类型，通过setMemoryStore 方法设置，**可以看作是Spark存储内存管理的接口。**

![1606700888754](.\images\1606700888754.png)

acquireMemory 方法最终调用的是MemoryStore 中的**evictBlocksToFreeSpace** 方法，将内存中的数据块落盘，回收相应的内存空间。当需要给计算执行层“借”内存时， StorageMemoryPool 中的**freeSpaceToShrinkPool** 方法提供了“收缩（ Shrink）”存储内存区可用内存来支持。具体实现如以下代码所示，该方法最终仍然调用MemoryStore 中的evictBlocksToFreeSpace 方法。

##### Spark存储模块

对于Spark 来讲，存储模块解藕了RDD 与底层物理存储，并提供了RDD 的持久化功能。StorageLevel 类中定义了持久化的不同维度，其构造参数是一个五元组（**＿useDisk,_useMemory＿useOffHeap, _deserialized, _replication** ），除__replication 为int 类型（默认为1 ）外，其他都是Boolean 类型。这种数据的存储方式包括磁盘、堆内内存和堆外内存3 种，存储形式可以是序列化和非序列化，副本数量大于1 时需要远程备份到其他节点。

![1606701675312](.\images\1606701675312.png)

具体到实现层面，**存储模块的入口为BlockManager 类**，整体架构采用的是**主从模式**,Driver 端的BlockManager 为主， Executor 端的BlockManager 为从）。数据存储的基本单位为数据块（ Block），每个Block 都会产生唯一的Blockid 标识。应用执行过程中， Driver端的BlockManager 负责对全部数据块（ Block）的元数据信息进行管理和维护， Executor 端的BlockManager 将数据块（ Block）的更新等状态上报到Driver 端，并接收主节点的相关操作命令。

**Spark 的RDD 在缓存到存储内存之前，每条数据的对象实例都处于JVM 堆内内存的Other部分**，即便同一个分区内的数据在内存空间中也是不连续的，具体分布由JVM 管理。上层通过Scala 中的迭代器来访问。当RDD 持久化到存储内存之后， **Partition 对应转换为Block** ，此时数据在存储内存空间（堆内或堆外）中将连续的存储。这里将Partition 由**不连续的存储空间转换为连续的存储空间**的过程，就是前面所提到的“**展开（ Unroll ）**”操作。

根据定义的存储级别（ StorageLevel), Block 也有序列化和非序列化两种存储格式。**非序列化的Block 定义在DeserializedMemoryEntry 类中，用一个数组存储所有的对象实例**；**序列化的Block 则以SerializedMemoryEntry 类来定义，用字节缓冲区（ ByteBuffer ）存储二进制数据**。每个Executor 的存储模块采用LinkedHashMap 数据结构来管理堆内和堆外存储内存中所有的Block对象的实例，对该LinkedHashMap 新增和删除间接记录了内存的申请和释放。

因为不能保证存储空间可以一次容纳Iterator 中所有的数据，所以当前的计算任务在执行“展开”操作时需要向MemoryManager 申请足够的空间来临时占位， 空间不足则展开失败，空间足够时可以继续进行。**对于序列化的Partition ，其所需的展开空间可以直接累加计算， 一次申请**；**而非序列化的Partition 则要在遍历数据的过程中依次申请，即每读取一条Record ，采样估算其所需的展开空间并进行申请，空间不足时可以中断，释放己占用的展开空间**。如果最终展开成功， 则**当前Partition 所占用的展开空间被转换为正常的缓存RDD 的存储空间。**

**同一个Executor 的所有计算任务共享有限的存储内存空间**，当有新的Block 需要缓存但是剩余空间不足且无法动态占用时，就要对LinkedHashMap 中的旧Block 进行淘汰（ Eviction ） 。**对于被淘汰的Block ，如果其存储级别中同时包含存储到磁盘的要求，则要对其进行落盘，否则直接删除该Block**，存储内存的淘汰规则如下：

- 被淘汰的旧Block 要与新Block 的内存模式（ MemoryMode ）相同，即同属于堆外或堆内内存。

- 新旧Block 不能属于同一个RDD ，避免循环淘汰。
- 旧Block 所属RDD 不能处于被读状态，避免引发一致性问题
- 遍历LinkedHashMap 中的Block ，按照最近最少使用（ LRU）的顺序淘汰，直到满足新Block 所需的空间。其中， LRU 是LinkedHash Map 的特性。



#### 执行内存管理

执行内存管理的入口是**ExecutionMemoryPool**，ExecutionMemoryPool 继承自MemoryPool ，构造参数是用于加锁的对象（ Lock）和内存模式（ MemoryMode ） 。ExecutionMemoryPool 内部包括poolName 和memoryForTask 两个变量，其中poolName 是内存池的名称(on-heap execution 或off-heap execution), **memoryForTask 实际上是一个HashMap 结构，记录了每个Task 的内存使用量**。

Executor 内运行的任务同样共享执行内存。假设当前Executor 中正在执行的任务数目为n ，**那么每个任务可占用的执行内存大小的范围为［1／2n， 1 ／n]**。**每个任务在启动时，要向Memory Manager 申请最少1/2n 的执行内存，如果不能满足要求， 则该任务被阻塞**，直到有其他任务释放了足够的执行内存， 该任务才能被唤醒。

**执行内存主要用于满足Shuffle 、Join 、Sort、Aggregation 等计算过程中对内存的需求**。

![1606703650915](.\images\1606703650915.png)

##### Shuffle过程执行内存使用

***Shuffle 实现方式的演化历程***

![1606703770253](.\images\1606703770253.png)

***HashShuffle***

关于Hash 方式的Shuffle 实现，顾名思义，其主要思想是按照Hash 的方式在每个map 端（对应shuffle write ）的任务中为每个reduce 端（对应shuffle read ）的任务生成一个文件。因此，如果有M 个map 任务， R 个reduce 任务，就会产生M × R 个文件，海量数据场景下会导致大量的磁盘IO 操作与内存开销。虽然有一些优化手段（例如，在0.8.l 版本中提出的文件合并），但是难以从根本上解决瓶颈问题。

***SortShuffle***

Sort Shuffle 的原理如图，这里的map 任务在Spark中称为ShuffleMapTask ，负责Shuffle 写的动作。ShuffleMapTask 中会根据（ partition id, key)对所有的key-value 记录进行排序，并将所有分区的数据写在一个文件中。在创建数据文件的同时， ShuffleMapTask 也会创建一个索引文件(i ndex ）来记录每个分区的大小和偏移量。

Sort Shuffle 的入口是SortShuffleManager, **ShuffleMapTask 会从SortShuffleManager 中得到Shuffle Writer 对象来执行写操作**，需要注意的是**写操作会将数据写在本地目录中**；对应的，在Shuffle 执行读任务时，**每个Task 会从SortShuffleManager 中获得ShuffleReader 对象**，由该对象到特定的服务器节点上读取特定的数据。

![1606704135498](.\images\1606704135498.png)

**Spark 在启动时会创建ShuffleManager 来管理Shuffle 过程**，默认情况下SortShuffleManager 是ShuffleManager 的具体实现（“ sort”和“Tungsten-sort”都对应SortShuffleManager ），其创建过程在SparkEnv 类中，如以下代码所示。可以看到， **Spark 采用的是一种插件式的Shuffle 模块管理方式，开发人员可以实现自己的ShuffleManager ，并修改对应的配置参数（ spark.shuffle.manager ）** 。

有了ShuffleManager 之后，从中获取ShuffleReader 和ShuffleWriter 对象来执行读和写操作ShuffieManager、Shuffle Writer 和ShuffleReader 都是trait 类型，其中定义的方法也非常容易理解。ShuffleReader 只有BlockStoreShuffleReader 一种实现， ShuffleWriter 有BypassMergeSortShuffie Writer 、SortShuffleWriter 和UnsafeShuffleWriter 3 种实现方式，从ShuffleManager 获取ShuffleWriter 时会根据相关信息自动选择一种。

![1606704387305](.\images\1606704387305.png)

SortShuffle的具体原理可查看书籍，[Spark Shuffle 解析-Hash Shuffle和Sort Shuffle](https://blog.csdn.net/qq_41775852/article/details/104805354)，[ Spark Sort-Based Shuffle源码分析](https://blog.csdn.net/qq_41775852/article/details/104909711)。

Shuffle 的Write 和Read 两个阶段对执行内存的使用：

- Shuffle Write ：若在map 端选择普通的排序方式，则会采用ExternalSorter 进行外排，在内存中存储数据时主要占用堆内执行空间。**若在map 端选择Tungsten 的排序方式，则采用ShuffleExternalSorter 直接对以序列化形式存储的数据进行排序，在内存中存储数据时可以占用堆外或堆内执行空间，取决于用户是否开启了堆外内存，以及堆外执行内存是否足够。**
- Shuffle Read ：在对reduce 端的数据进行聚合时，**要将数据交给Aggregator 处理，在内存中存储数据时占用堆内执行空间**。如果需要进行最终结果排序，则要再次**将数据交给ExternalSorter 处理，占用堆内执行空间。**

![1606705275504](.\images\1606705275504.png)

在ExternalSorter 和Aggregator 中， **Spark 会使用一种叫作AppendOnly Map 的哈希表在堆内执行内存中存储数据**，但在Shuffle 过程中所有的数据并不能都保存到该哈希表中，这个哈希表占用的内存会进行周期性地采样估算，当其大到一定程度，无法再从MemoryManager 申请到新的执行内存时， **Spark 就会将其全部内容存储到磁盘文件中，这个过程被称为溢存（ Spill ）， 溢存到磁盘的文件最后会被归并（ Merge ）** 。

### Tungsten 内存管理优化基础

催生Tungsten 内存管理优化的原因主要来自两个方面。

- Java 对象占用内存空间大。相对于CIC ＋＋ 等更加底层的程序语言， Java 对象的存储密度相对偏低。例如，即使最简单的“abed” 字符串，用Java 的UTF-16 编码的情况下也需要8字节进行存储， 加上Java 内存布局的其他信息（如h eader 等），共需要48 字节的空间来存储“abed”字符串。
- JVM 垃圾回收（ Garbage Collection ）的开销大。在海量数据场景下，数据分析通常会涉及转换、清洗、处理等步骤，这个过程伴随着海量的Java 对象创建与回收， JVM 垃圾回收的执行效率对应用性能有着很大影响。

Tungsten内存分配管理的基础是**MemoryAllocator 接口**，定义了allocate 和free 两个方各分别来申请内存和释放内存。（**MemoryAllocator 和MemoryPool的区别：MemoryPool只是用数字记录了内存的使用情况，没有具体分配内存空间。而MemoryAllocator 是真正的分配了内存空间，如堆内堆外字节数组。所以Tungsten内存分配时，首先调用MemoryPool判断是否还有内存可以分配并记录此次需要分配的内存，然后再调用MemoryAllocator分配内存字节数组。**）MemoryAllocator 类的具体实现包括HeapMemoryAllocator （堆内内存分配）UnsafeMemoryAllocator （堆外内存分配）两种。

![1606706644670](.\images\1606706644670.png)

内存管理必然涉及内存的寻址，因此内存地址的表示方式是重要的部分。**Spark 中定义MemoryLocation 来统一记录和追踪堆内、堆外的内存地址**：在堆内模式中，内存由该对象的引用（ obj ）和64bit 的偏移量（ offset ）去寻址；在堆外模式中，内存可以直接通过64bit 长度的地址进行寻址（将o 问设置为null ） 。**MemoryBlock 用来表示一段连续的内存块，在MemoryLocation 的基础上增加了内存页编号（ pageNumber ）和对应数据的大小(length ） 。**

两种Memory Allocator 的实现对比如图 所示。首先是HeapMemory Allocator ，**从allocate方法的实现可以看到，以8 字节对齐的方式申请长度为（ size 十7) /8 的长整型数组(array）；然后构造MemoryBlock 对象，其obj 成员即为array, offset 成员为Platform 类中得到的LONG_ARRAY _OFFSET 指标**。堆内内存的释放一般不做任何操作，**直接交给JVM 的垃圾回收**。实际上，当申请的数据量比较大时，会有一个内存池来管理，内存池中预先存放各种尺寸的内存块。

依赖于Unsafe 包， Spark 将相关的一系列Unsafe 操作作为静态方法统一封装在Platform 类中。因此， **UnsafeMemoryAllocator 中直接使用Platform 的allocateMemory 方法分配内存**，这个方法是本地化（ Native ）的实现，即通过JNI 最终调用C 和C ＋＋ 实现，类似在C 语言中新建一个数组，得到的是绝对内存地址。堆外内存的回收也需要Spark 来完成， **UnsafeMemoryAllocator使用的是Platform 的freeMemory 方法。**

![1606707395115](.\images\1606707395115.png)

**Tungsten 内存管理机制较核心部分的实现在TaskMemoryManager 类中**。 **Tungsten 按照内存页表（ pageTable）的方式来管理内存，pageTable 本质上是一个MemoryBlock 的数组**，这些内存会被Task 内部的内存消费者（ MemoryCo n sumer ）使用。

![1606707488218](.\images\1606707488218.png)

每个Task 的内存空间被划分为多个内存页（ page ） ，每个内存页本质上都是一个内存块(MemoryBlock ） 。为统一堆内和堆外的内存访问方式， TaskMemoryManager 引入了类似操作系统中虚拟内存逻辑地址的概念，并将逻辑地址映射到实际的物理地址。**逻辑地址由一个64bits的长整型表示，其中处于高位的13bits 用来表示页编号（page Number），处于低位的51bits 用来表示在该内存页内部的偏移（offsetlnPage）** 。**这样内存映射的过程，实际上就是先根据内存页编号查询页表（pageTable），得到对应的内存页，然后得到该页的物理地址，最后在物理地址上加上偏移，得到实际内存物理地址。**因此，所有内存地址都可由pageNumber 和offsetinPage 决定。

### Tungsten 内存优化应用

#### Shuffle 实现： UnsafeShuffleWriter/ShuffleExternalSorter/ShufflelnMemorySorter

**Tungsten 方式的Shuffle的优势**：

- ShuffleExternalSorter使用UnSafe API操作序列化数据，而不是Java对象，减少了内存占用及因此导致的GC耗时(参考[Spark 内存管理之Tungsten](http://blog.csdn.net/u011564172/article/details/71170176))，这个优化需要Serializer支持relocation。
- ShuffleExternalSorter存原始数据，ShuffleInMemorySorter使用压缩指针存储元数据，每条记录仅占8 bytes，并且排序时不需要处理原始数据，效率高。
- **溢写 & 合并**这一步操作的是同一Partition的数据，因为使用UnSafe API直接操作序列化数据，合并时不需要反序列化数据。
- **溢写 & 合并**可以使用**fastMerge**提升效率(调用NIO的transferTo方法)，设置**spark.shuffle.unsafe.fastMergeEnabled**为true，并且如果使用了压缩，需要压缩算法支持SerializedStreams的连接

Tungsten 方式的Shuffle 过程中， ShuffleMapTask 的输出数据能够先序列化为二进制数据存储在内存中，再执行相关的操作。Tungsten Shuffle 的写操作由UnsafeShuffle Writer 完成，**与常规的SortShuffleWriter 的不同之处在于UnsafeShuffleWriter 中不涉及数据的反序列化操作。**

UnsafeShuffle Writer 里面维护着一个ShuffleExternalSorter ，用来进行外部排序，与SortShuffieWriter 中的ExternalSorter 功能类似。当UnsafeShuffleWriter 在逐条写入RDD 的（ K,V ）记录时，首先会由Partitioner 根据K 得到partitionld ，并依次将K 和V 序列化写入临时的serOutputStream中，然后写入ShuffleExternalSorter 。因此， **ShuffleExternalSorter 是Tungsten Shuffle 写实现的核心所在**。

ShuffleExternalSorter 写入数据的过程如图所示，可以看到， 其内部包括申请到的内存页链表allocatedPages (LinkedList<MemoryBlock> 类型），每个Page 中会存储由（ K,V）数据经过处理后得到的记录（ record) , currentPage 代表当前的内存页。此外， **ShuffleExternalSorter 还需要对数据进行排序，这部分工作由ShuffleinMemorySorter 即inMemSorter 完成**，其内部包含了一个存储数据指针的LongArray 数组。

ShuffleinMemorySorter 的LongArray 数组中存放的是经过编码后的数据（recordPointer, partitionld）。如图所示，编码的工作由PackedRecordPointer 中的packPointer 方法完成，对于输入的长整型recordPointer 和整型的partitionld ，得到的结果为8 字节（ 64 位）的长整型，其中24 位用来表示分区的编号， 13 位表示内存页的编号，剩余的27 位表示页内偏移量。

![1606717071592](.\images\1606717071592.png)



当写入一条数据到ShuffleExternalSorter 中时：

1. 首先会检查能否插入到inMemSorter 中ShufileinMemorySorter 内存存储的数据有一定的阔值，默认是1024 × 1024 × 1024 =1073741824 条，可以通过参数spark.shufile.spill . numElementsForceSpillThreshold 设置。因此，如果当前inMemSorter 中的数据量达到了这个阔值，**会首先执行spill 操作，将内存中的数据写入磁盘**，以便释放内存空间。
2. 检查了阔值条件之后，还需要调用growPointer Array IfN ecessary 方法检查inMemSorter 中的LongArray 是否有足够的内存空间容纳新数据生成的指针，如果没有，则将LongArray 的大小扩充到当前大小的两倍。接下来检查当前的内存空间能否存储新的数据（加上存储数据长度的4 字节），这一步由acquireNew PageifN ecessary 方法完成，如果无法满足，则ShufileExternalSorter 会申请新的内存页。
3. 调用Platform 的putlnt 方法向内存空间中插入（ K,V ）数据的长度，然后调用Platform 的copyMemory 方法将（ K,V ）数据本身写入内存空间中，最终写入的数据是（ len+K+V ） 。此外，**在写入数据的同时，会由TaskMemoryManager 编码得到数据指针recordAddress ，与partitionld 一起作为参数插入到ShuffleinMemorySorter 中**。至此，ShufileExternalSorter 写入一条数据的流程完毕。
4. **spill操作前，需要对数据进行排序**。ShuffleinMemorySorter 完成内存数据排序的方法有两种： 一种是**基数排序** ,可以通过参数设置（ spark.shuffle. sort. useRadixSort ），默认为true ；另一种是**TimSort** ，对归并算法进行大量的优化。两种排序空间复杂度不一样，因此ShuffieinMemorySorter 在计算LongArray 的可用空间（ getUsableCapacity）时，会考虑不同的算法。假设LongArray 长度为n,如果是RadixSort ，则LongArray 的可用空间为n/l.5 ；如果是TimSort ，则可用空间是n/2 。
5. spill操作操作由UnsafeSorterSpillWriter完成。此时， UnsafeShuffieinMemorySorter 生成一个数据迭代器（ getSortedlterator ），**该迭代器中的数据已经是按照partitionld 排好序的，迭代器得到的每个元素都是一个指针，对应上面提到的经过PackedRecordPointer 编码后的地址，通过该地址可以很方便地获取原始数据**。需要注意的是，**数据在最初写入UnsafeShuffieExternalSorter 时已经被序列化，因此spill 操作此时就是单纯地写字节数组**。一个文件里不同的partiton 数据会用fileSegment 形式来表示，对应的信息存在SpillInfo 数据结构中， SpillInfo 中包含文件信息、记录每个分区大小的长整型数组和blockld 3 个元素。此外， spill 操作完成后会释放内存。
6. 当完成所有数据的写入之后，会调用closeAndWriteOutput 方法进行spill 文件合并操作。文件的合并可以通过fastMerge 机制来提升效率，将参数spark.shuffle. unsafe.fastMergeEnabled 设置为true ，如果使用了压缩， 需要压缩算法支持Serialized Streams 的连接，**可以直接、简单地将相同分区的压缩数据连接到一起，而且不用进行解压和反序列化操作**。

![1606716691442](.\images\1606716691442.png)

## 缓存敏感计算（ Cache-aware computation)

缓存敏感计算（ Cache-aware computation ）是Tungsten 中一个比较重要的优化，主要对比的是普通的内存计算。在硬件层面，访问CPU 的Ll/L2/L3 级缓存比访问内存速度快，大数据处理系统可以利用这个特性优化性能。而要将数据存储在Ll/L2/L3 中，数据大小需要满足特定条件。

 Tungsten 缓存敏感计算机制通过设计缓存友好的数据结构来提高缓存命中率(Cache hit ）和本地化（ Cache locality）的特性。到目前为止， **Spark 中缓存敏感的计算优化针对的主要是排序（ Sort）操作**，相应的实现是**UnsafeExternalSorter 和UnsafeinMemorySorter** 类。

Tungsten 中cache-aware 排序原理如图所示。常规的做法是每个record ( <key， value> )有一个指针指向该record ，对两个record 排序先根据指针定位到实际数据，然后对实际数据进行比较，这个操作涉及的都是内存的随机访问（ Random access ），缓存本地化（ Cache locality)会变得很低。

在实现上， cache aware 的排序功能由UnsafeExternalSorter 和UnsafeinMemorySorter 来完成。UnsafeExternalSorter 中包含一个UnsafeinMemorySorter ，用于内存中的数据排序，两者之间的关系与ShuffleExternalSorter 和ShuffleinMemorySorter 类似。UnsafeinMemorySorter 存储数据指针和数据前缀。**对两条记录进行排序时，首先判断两条记录的prefix 是否相等，如果根据prefix 就可以判断出两条记录的大小，那么直接返回结果；否则根据数据指针得到的实际数据再进行进一步的比较。**相对于实际数据， pointer 和prefix 占用的空间都比较小，**在比较时遍历较小的数据结构更有利于提高cache 命中率**。

总的来看， UnsafeExterna!Sorter 和ShuffleExternalSorter 中涉及的各种概念类似，最大的区别在于**数据插入方法中是否需要指定前缀信息**（包括前缀本身prefix 和判断前缀是否为null 的prefixisNull ） 。数据的插入分为两部分， **一部分是真实数据的插入，另一部分是其索引的更新**。每条数据的索引包含了真实数据插入的地址和其prefix 值，**当记录只用prefix 来比较大小时，真实数据的排序就转换为索引的排序**。真实数据的插入调用Platform 中的方法，数据会加上length ， 每插入一条数据都会检查内存是否足够，

***缓存友好：***

**将实际的数据和指针加定长key分开存放有两个目的。第一，交换定长块（key+pointer）更高效，不用交换真实的数据也不用移动其他key和pointer。第二，这样做是缓存友好的，因为key都是连续存储在内存中的，可以大大减少 cache miss**

## 动态代码生成

当今绝大多数数据库系统处理SQL 查询的方式都是将其翻译成一系列的关系代数算子或表达式，然后依赖这些关系代数算子逐条处理输入数据并产生结果。从本质上看，这是一种迭代的模式。该模式可以概括为．每个物理关系代数算子反复不断地调用next 函数来读入数据元组(Tuple ）作为算子的输入，经过表达式处理后输出一个数据元组的流（Tuple stream ） 。这种模式简单而强大，能够通过任意组合算子来表达复杂的查询。这种迭代处理模式提出的背景是减轻查询处理的IO，对CPU 的消耗则考虑较少 。首先，每次处理一个数据元组时， next 函数都会被调用，在数据量非常大时，调用的次数会非常多。最重要的是， **next 函数通常实现为虚函数或函数指针，这样每次调用都会引起CPU 中断。并使得CPU 分支预测（ Branch Prediction ）下降**，因此相比常规函数的调用代价更大。此外，迭代处理模式通常会**导致很差的代码本地化能力，并且需要保存复杂的处理信息**。例如，表扫描算子在处理一个压缩的数据表时，在迭代模式下，需要每次产生一个数据元组，因此表扫描算子中需要记录当前数据元组在压缩数据流中的位置，以便根据压缩方式跳转到下一条数据元组的位置。

总的来讲，动态代码生成能够有效地解决3 个方面的问题：

- 大量虚函数调用，生成的实际代码不再需要执行表达式系统中统一定义的虚函数（如Eval、Evaluate 等） 。
- 判断数据类型和操作算子等内容的大型分支选择语句。
- 常数传播（ Constants propagation ）限制，生成的代码中能够确定性地折叠常量。