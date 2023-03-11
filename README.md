# study_elk

ELK是包含但不限于Elasticsearch（简称es）、Logstash、Kibana
三个开源软件的组成的一个整体，分别取其首字母组成ELK。ELK是用于数据抽取（Logstash）、搜索分析（Elasticsearch）、数据展现（Kibana）的一整套解决方案，所以也称作ELK stack。

# 经典白学

POST /_sql?format=txt {
"query":"select * from tvs"
}

POST /_sql?format=txt {
"query":"select color,avg(price),max(price),min(price),sum(price) from tvs group by color"
}

POST /_sql?format=txt {
"query":"show tables"
}

POST /_sql?format=csv {
"query":"show tables"
}

POST /_sql?format=tsv {
"query":"show tables"
}

POST /_sql?format=json {
"query":"show tables"
}

POST /_sql?format=yaml {
"query":"show tables"
}
