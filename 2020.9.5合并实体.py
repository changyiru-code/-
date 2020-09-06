#该代码正确，可以合并节点
import csv
from py2neo import Graph,Node,Relationship,NodeMatcher
#读取第一个文件
def file1(csv_file1,graph):
    csv_list1 = list(csv_file1)
    for i in range(1, len(csv_list1)):  # len(csv_list)
        if len(csv_list1[i]) != 0:
            print(csv_list1[i])
            # 创建题目实体节点
            Title = Node('title', Title_name=csv_list1[i][0])
            # 在数据库中创建节点
            graph.merge(Title, 'title', 'Title_name')
            # 创建作者实体节点
            Author = Node('author', Author_name=csv_list1[i][1])
            # 在数据库中创建节点
            graph.merge(Author, 'author', "Author_name")
            # 创建学校实体节点
            Institute = Node('institute', Institute_name=csv_list1[i][2])
            # 在数据库中创建节点
            graph.merge(Institute, 'institute', "Institute_name")
            # 创建实体关系类型节点
            author_of = Relationship.type("author_of")
            institute_of = Relationship.type("institute_of")
            # 在图形数据库中创建实体和关系
            graph.merge(author_of(Title, Author), "title", "Title_name")
            graph.merge(institute_of(Author, Institute), "title", "Title_name")
#读取第二个文件
def file2(csv_file2,graph):
    csv_list2 = list(csv_file2)
    for i in range(1, len(csv_list2)):  # len(csv_list)
        if len(csv_list2[i]) != 0:
            print(csv_list2[i])
            # 题目实体节点已经存在，无需创建
            Title = Node('title', Title_name=csv_list2[i][0])
            # 创建关键词实体节点
            Keywords = Node('keywords', Keywords_name=csv_list2[i][1])
            # 在数据库中创建节点
            graph.merge(Keywords, 'keywords', "Keywords_name")
            # 创建实体关系类型节点
            keywords_of = Relationship.type("keywords_of")
            # 在图形数据库中创建实体和关系
            graph.merge(keywords_of(Title, Keywords), "title", "Title_name")
#读取第三个文件
def file3(csv_file3,graph):
    csv_list3 = list(csv_file3)
    for i in range(1, len(csv_list3)):  # len(csv_list)
        if len(csv_list3[i]) != 0:
            print(csv_list3[i])
            # 创建题目实体节点
            Title = Node('title', Title_name=csv_list3[i][0], Abstract_content=csv_list3[i][4])
            graph.merge(Title, 'title', 'Title_name')

#读取第四个文件
def file4(csv_file,graph):
    csv_list = list(csv_file)
    for i in range(1, len(csv_list)):  # len(csv_list)
        if len(csv_list[i]) != 0:
            print(csv_list[i])
            # 创建实体1节点
            entity1 = Node('entity1', entity_1=csv_list[i][0])
            # 在数据库中创建节点
            graph.merge(entity1, 'entity1', "entity_1")
            # 创建实体2节点
            entity2 = Node('entity2', entity_2=csv_list[i][2])
            graph.merge(entity2, 'entity2', "entity_2")
            rel = Relationship.type(csv_list[i][1])
            # 在图形数据库中创建实体和关系
            graph.merge(rel(entity1, entity2),"entity1", "entity_1")


def main():
    graph = Graph("http://localhost:7474", username="neo4j", password='19980529')
    # csv 读取
    csv_file1 = csv.reader(open(
        'D:\\常艺茹的文档\\研究生期间的资料\\知识图谱\\Vulnerability-Knowledge-Graph-master\\Vulnerability-Knowledge-Graph-master\\漏洞demo\\作者.csv',
        'r', encoding='utf-8'))
    print(csv_file1)  # 打印出来的csv_file1只是一个对象的模型
    csv_file2 = csv.reader(open(
        'D:\\常艺茹的文档\\研究生期间的资料\\知识图谱\\Vulnerability-Knowledge-Graph-master\\Vulnerability-Knowledge-Graph-master\\漏洞demo\\关键词.csv',
        'r', encoding='utf-8'))
    print(csv_file2)  # 打印出来的csv_file2只是一个对象的模型
    csv_file3 = csv.reader(open(
        'D:\\常艺茹的文档\\研究生期间的资料\\知识图谱\\Vulnerability-Knowledge-Graph-master\\Vulnerability-Knowledge-Graph-master\\漏洞demo\\Bug.csv',
        'r', encoding='utf-8'))
    print(csv_file3)  # 打印出来的csv_file3只是一个对象的模型
    csv_file4 = csv.reader(open(
        'D:\\常艺茹的文档\\研究生期间的资料\\知识图谱\\Vulnerability-Knowledge-Graph-master\\Vulnerability-Knowledge-Graph-master\\漏洞demo\\output.csv',
        'r', encoding='utf-8'))
    print(csv_file4)  # 打印出来的csv_file1只是一个对象的模型
    file1(csv_file1, graph)
    file2(csv_file2, graph)
    file3(csv_file3, graph)
    file4(csv_file4, graph)
    # gql = 'MATCH (a:entity1),(b:entity2),(c:title),(d:keywords)  where a.entity_1 = b.entity_2 or a.entity_1 = c.Title_name or a.entity_1 = d.Keywords_name or b.entity_2 = c.Title_name or b.entity_2 = d.Keywords_name or c.Title_name= d.Keywords call apoc.refactor.mergeNodes([a,b,c,d]) YIELD node  RETURN node;'
    gql1 = 'MATCH (a:entity1),(b:entity2)  where (a.entity_1 = b.entity_2 and id(a) <> id(b))  call apoc.refactor.mergeNodes([a,b]) YIELD node  RETURN node;'
    gql2 = 'MATCH (a:entity1),(b:title)  where (a.entity_1 = b.Title_name and id(a) <> id(b)) call apoc.refactor.mergeNodes([a,b]) YIELD node  RETURN node'
    gql3 = 'MATCH (a:entity1),(b:keywords)  where (a.entity_1 = b.Keywords_name and id(a) <> id(b))  call apoc.refactor.mergeNodes([a,b]) YIELD node  RETURN node;'
    gql4 = 'MATCH (a:entity2),(b:title)  where (a.entity_2 = b.Title_name and id(a) <> id(b)) call apoc.refactor.mergeNodes([a,b]) YIELD node  RETURN node'
    gql5 = 'MATCH (a:entity2),(b:keywords)  where (a.entity_2 = b.Keywords_name and id(a) <> id(b)) call apoc.refactor.mergeNodes([a,b]) YIELD node  RETURN node'
    gql6 = 'MATCH (a:title),(b:keywords)  where (a.Title_name = b.Keywords_name and id(a) <> id(b)) call apoc.refactor.mergeNodes([a,b]) YIELD node  RETURN node'
    graph.run(gql1)
    graph.run(gql2)
    graph.run(gql3)
    graph.run(gql4)
    graph.run(gql5)
    graph.run(gql6)

main()
