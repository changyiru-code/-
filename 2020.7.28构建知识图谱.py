#该例程正确，无重复节点
import csv
from py2neo import Graph,Node,Relationship,NodeMatcher
graph = Graph("http://localhost:7474", username="neo4j", password='19980529')
#csv 读取
csv_file1=csv.reader(open('D:\\常艺茹的文档\\研究生期间的资料\\知识图谱\\Vulnerability-Knowledge-Graph-master\\Vulnerability-Knowledge-Graph-master\\漏洞demo\\作者.csv','r',encoding='utf-8'))
print(csv_file1)  #打印出来的csv_file1只是一个对象的模型
csv_file2=csv.reader(open('D:\\常艺茹的文档\\研究生期间的资料\\知识图谱\\Vulnerability-Knowledge-Graph-master\\Vulnerability-Knowledge-Graph-master\\漏洞demo\\关键词.csv','r',encoding='utf-8'))
print(csv_file2)  #打印出来的csv_file2只是一个对象的模型
csv_file3=csv.reader(open('D:\\常艺茹的文档\\研究生期间的资料\\知识图谱\\Vulnerability-Knowledge-Graph-master\\Vulnerability-Knowledge-Graph-master\\漏洞demo\\Bug.csv','r',encoding='utf-8'))
print(csv_file3)  #打印出来的csv_file3只是一个对象的模型


#读取第一个文件
csv_list1=list(csv_file1)
for i in range(1,len(csv_list1)): #len(csv_list)
    if len(csv_list1[i]) != 0:
        print(csv_list1[i])
        # 题目实体节点已经存在，无需创建
        Title = Node('题目', Title_name=csv_list1[i][0])
        # 在数据库中创建节点
        graph.merge(Title, '题目', 'Title_name')
        # 创建作者实体节点
        Author = Node('作者', Author_name=csv_list1[i][1])
        # 在数据库中创建节点
        graph.merge(Author, '作者', "Author_name")
        # 创建学校实体节点
        Institute = Node('学校', Institute_name=csv_list1[i][2])
        # 在数据库中创建节点
        graph.merge(Institute, '学校', "Institute_name")
        # 创建实体关系类型节点
        author_of = Relationship.type("author_of")
        institute_of = Relationship.type("institute_of")
        # 在图形数据库中创建实体和关系
        graph.merge(author_of(Title, Author), "题目", "Title_name")
        graph.merge(institute_of(Author, Institute), "题目", "Title_name")
#读取第二个文件
csv_list2=list(csv_file2)
for i in range(1,len(csv_list2)): #len(csv_list)
    if len(csv_list2[i]) != 0:
        print(csv_list2[i])
        # 题目实体节点已经存在，无需创建
        Title = Node('题目', Title_name=csv_list2[i][0])
        # 创建关键词实体节点
        Keywords = Node('关键词', Keywords_name=csv_list2[i][1])
        # 在数据库中创建节点
        graph.merge(Keywords, '关键词', "Keywords_name")
        # 创建实体关系类型节点
        keywords_of = Relationship.type("keywords_of")
        # 在图形数据库中创建实体和关系
        graph.merge(keywords_of(Title, Keywords), "题目", "Title_name")
#读取第三个文件
csv_list3=list(csv_file3)
for i in range(1,len(csv_list3)): #len(csv_list)
    if len(csv_list3[i]) != 0:
        print(csv_list3[i])
        # 创建题目实体节点
        Title = Node('题目', Title_name=csv_list3[i][0], Abstract_content=csv_list3[i][4])
        graph.merge(Title, '题目', 'Title_name')
        # # 创建题目实体节点,该方法与上面一样的效果
        # Title = Node('题目', **{"Title_name":csv_list3[i][0], "Abstract_content":csv_list3[i][4]})
        # # 在数据库中创建节点
        # graph.create(Title)

#试一下创建关系前判断节点是否存在，