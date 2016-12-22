工程目录：

    /src

        TotalDriver.java
        /dataReduction

            LabelSpreadDataReduction.java

        /gephiProcess

            GephiNormalProcess.java

            GephiSecondProcess.java

        /labelSpread

            LabelSpreadPreprocess.java

            LabelSpreadProcess.java

            LabelSpreadViewer.java

        /peopleRank

            GraphBuilder.java

            PeopleRankIterator.java

            PeopleRankViewer.java

         /splitWords

            DataNormalization.java

            PeopleConCurrence.java

            splitWordsByAnsj.java

源代码功能说明：

    TotalDriver.java			串联所有 MapReduce 程序

    LabelSpreadDataReduction.java	标签传播结果整理

    GephiNormalProcess.java		生成可视化点数据(非 MapReduce 程序)

    GephiSecondProcess.java		生成可视化边数据(非 MapReduce 程序)

    LabelSpreadPreprocess.java		标签传播预处理

    LabelSpreadProcess.java		标签传播迭代处理

    LabelSpreadViewer.java		标签传播结果输出

    GraphBuilder.java			PageRank 预处理

    PeopleRankIterator.java		PageRank 迭代处理

    PeopleRankViewer.java		PageRank 结果输出

    DataNormalization.java		人物关系图构建与特征归一化

    PeopleConCurrence.java		人物共现统计

    splitWordsByAnsj.java		分词处理



编译环境：

    Eclipse 4.5.2
 
   jdk 1.7.0_79



编译方法：

    将源代码 Export 成 Runnable JAR file, 我们编译为 TotalDriver.jar 。



运行环境：

    hadoop-2.7.1

    idk 1.7.0_79



集群上jar包的执行方式：

    hadoop jar TotalDriver.jar /data/task2/novels SplitWordsOut PeopleConOut DataNorOut PROunt LSOut DataRedOunt

    其中 SplitWordsOut PeopleConOut DataNorOut PROunt LSOut DataRedOunt 为输出目录，事先不能存在。



输出目录说明：

    SplitWordsOut	存放分词结果

    PeopleConOut	存放人物共现关系结果

    DataNorOut		存放人物共现关系归一化结果

    PROunt		存放人物 PageRank 值计算结果，按照从大到小序排列

    LSOut		存放标签传播算法结果

    DataRedOut		存放标签传播整理结果

