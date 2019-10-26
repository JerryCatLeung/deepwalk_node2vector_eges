# deepwalk_node2vector_eges
将deepwalk、node2vector和阿里的文章:Billion-scale Commodity Embedding for E-commerce Recommendation in Alibaba
用代码实现，将随机游走与embedding分开，其中代码中：
1. 主函数Main1通过设置p和q来随机游走
2. 主函数Main2（全量同步训练）是word2vector与阿里文章的结合，当边信息长度为1时就是word2vector，边信息长度大于1时就是阿里文章代码
3. 主函数Main3（分多批次同步训练）是与主函数Main2一样，只不过是多批训练