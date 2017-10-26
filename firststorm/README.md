<h6>Wordcount 实例</h6>



	第一次接入jstorm开发

	------------------
	

	看了一天的官方文档(https://github.com/alibaba/jstorm/wiki/JStorm-Chinese-Documentation)
	理解其必须要实现的Spout bolt 以及主入口的topology
	
	准备入手jstorm项目,实现一个最简单的Wordcount
	-----------------	
	程序描述
		spout随机发送一个准备好的需要计算的文档(单词的文档)，并在spout的nextTuple方法中用collector发射每一行数据到splitbolt
		第一层SplitBolt，负责对spout发过来的数据进行split,分解成独立的单词，并按照一定的规则发往下一层CountBolt处理
		第二层CountBolt，接收第一层bolt传过来的数据，并对各个单词进行数量计算
	程序流程
		spout数据源
		bolt1进行split操作
		bolt2进行count操作
		Topolgy运行程序
	程序运行
	mvn package 
	在cluster中执行:jstorm jar hello.jstorm-1.0-SNAPSHOT.jar com.sijian.topology.FirstTestTopology firstjstorm /home/user1/qiangs/world1.txt /home/user1/qiangs/test.out
	
	-----------------
	参考文档:
	文档1 (http://www.cnblogs.com/chen-kh/p/5975683.html)
	文档2 (http://www.mamicode.com/info-detail-1767802.html)


