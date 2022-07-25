import re
import os
import math
import glob
import json
import networkx as nx
import matplotlib.pyplot as plt

# Newline variable for f-strings
nl = '\n'

input_folder = './COVID-19-Analysis/'
project_name = ''

graph_name = ''
verts = []
edges = []
vert_names = []
edge_names = []
schema = {}

def get_folder_size(path, mdOut):
    folder_size = 0
    for dirpath, dirnames, filenames in os.walk(path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            if not os.path.islink(fp):
                folder_size += os.path.getsize(fp)
    folder_size = convert_size(folder_size)
    mdOut.write('## Data Size\n')
    mdOut.write(f'All Files: {folder_size}{nl}')
    return folder_size

def convert_size(size_bytes):
   if size_bytes == 0:
       return "0B"
   size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
   i = int(math.floor(math.log(size_bytes, 1024)))
   p = math.pow(1024, i)
   s = round(size_bytes / p, 2)
   return "%s %s" % (s, size_name[i])

# print(get_folder_size(input_folder + 'GlobalTypes/'))

def parse_schema():
    global schema
    global verts
    global edges

    schema = {}
    verts = []
    edges = []
    schema_file = open(input_folder + 'global.gsql', 'r')

    for line in schema_file:
        # see if our line is dealing with a vertex or edge
        vertex_name = re.search(r'(?:create|CREATE) (?:vertex|VERTEX) ([\w\_\-]*)', line)
        edge_name = re.search(r'(?:create|CREATE) (\w*) (?:edge|EDGE) ([\w\_\-]*)', line)
        if vertex_name != None:
            vertex = {}
            vertex_name = vertex_name.group(1)
            vert_names.append(vertex_name)
            # we already have the name so we only need to deal with what comes after it
            details = re.search(r'\(.*$', line).group(0)
            primary_id = re.search(r'(?:PRIMARY_ID|primary_id) ([\w\_\-]*) (\w*)', details)
            vertex = {"name": vertex_name, "primary_id": {"name": primary_id.group(1), "type": primary_id.group(2)}}
            # get a list of attribute - type combos
            attributes = re.findall(r', ([\w\_\-]*) (\w*)', details)
            attrs = []
            for attribute in attributes:
                att = {"name": attribute[0], "type": attribute[1]}
                attrs.append(att)
            vertex["attributes"] = attrs
            # get everythong after the closing ')'
            additional_details = re.search(r'\)\s?(?:with|WITH)(.*)', details).group(1)
            additional_details = re.findall(r'(\w*)=\"([\w\_\-]*)\"', additional_details)
            addl_deets = []
            for addl_deet in additional_details:
                vertex[addl_deet[0]] = addl_deet[1]
            verts.append(vertex)
        elif edge_name != None:
            edge = {}
            directed = edge_name.group(1)
            directed = True if directed in ["DIRECTED","directed"] else False
            edge_name = edge_name.group(2)
            edge_names.append(edge_name)
            details = re.search(r'\(.*$', line).group(0)
            attributes = re.findall(r'(?:\(|\,\s?)([\w\_\-]*) ([\w\_\-]*)', details)
            attrs = []
            to_vert = ""
            from_vert = ""
            for attribute in attributes:
                if attribute[0] in ["FROM","from"]:
                    from_vert = attribute[1]
                elif attribute[0] in ["TO","to"]:
                    to_vert = attribute[1]
                else:
                    att = {"name": attribute[0], "type": attribute[1]}
                    attrs.append(att)
            edge = {"name": edge_name, "from": from_vert, "to": to_vert, "directed": directed, "attributes": attrs}
            try:
                additional_details = re.search(r'\)\s?(?:with|WITH)(.*)', details).group(1)
                additional_details = re.findall(r'(\w*)=\"([\w\_\-]*)\"', additional_details)
                addl_deets = []
                for addl_deet in additional_details:
                    edge[addl_deet[0]] = addl_deet[1]
            except:
                pass
            edges.append(edge)

    schema = {"nodes": verts, "edges": edges}
            
    schema_file.close()

def generate_graph(folder_path, mdOut):
    viz_graph = nx.MultiDiGraph()

    nodes = []
    for node in schema["nodes"]:
        attrs = {}
        for att in node["attributes"]:
            attrs[att["name"]] = att["type"]
        nodes.append(tuple((node["name"], attrs)))

    edges = []
    for edge in schema["edges"]:
        attrs = {}
        for att in edge["attributes"]:
            attrs[att["name"]] = att["type"]
        edges.append(tuple((edge["from"], edge["to"], 1.0, attrs)))


    viz_graph.clear()
    plt.clf()
    viz_graph.add_nodes_from(nodes)
    viz_graph.add_edges_from(edges)
    nx.draw_kamada_kawai(viz_graph, node_color=range(len(nodes)), cmap=plt.cm.tab20, edge_color='#545454', with_labels=True, font_weight='bold')
    plt.savefig(folder_path + 'schema.png')
    mdOut.write('## Schema\n')
    mdOut.write('![](schema.png)\n')
# print(json.dumps(schema, indent=2))

def add_schema_tables(mdOut):
    mdOut.write('<table>\n')
    mdOut.write('<tr><th>Nodes</th><th>Edges</th></tr>\n')
    mdOut.write('<tr><td>\n\n')

    for node in schema["nodes"]:
        mdOut.write(f'### {node["name"]}{nl}')
        mdOut.write('|Attribute|Type|\n')
        mdOut.write('|--|--|\n')
        mdOut.write(f'|**Primary id -** {node["primary_id"]["name"]}|{node["primary_id"]["type"]}|{nl}')
        for idx, attr in enumerate(node["attributes"]):
            mdOut.write(f'|{node["attributes"][idx]["name"]}|{node["attributes"][idx]["type"]}|{nl}')
        mdOut.write("\n")

    mdOut.write('</td><td>\n\n')

    for edge in schema["edges"]:
        mdOut.write(f'### {edge["name"]}')
        mdOut.write('\n')
        mdOut.write('|From Vert|To Vert|\n')
        mdOut.write('|--|--|\n')
        mdOut.write(f'|{edge["from"]}|{edge["to"]}|{nl}')
        mdOut.write('\n')
        if edge["attributes"] != []:
            mdOut.write('|Attribute|Type|\n')
            mdOut.write('|--|--|\n')
            for idx, attr in enumerate(edge["attributes"]):
                mdOut.write(f'|{edge["attributes"][idx]["name"]}|{edge["attributes"][idx]["type"]}|{nl}')
            mdOut.write("\n")
    mdOut.write('</td></tr> </table>\n\n')

def count_graph(input_folder, mdOut):
    node_count = {}
    edge_count = {}
    tot_nodes = 0
    tot_edges = 0
    dataFolder = input_folder + 'GlobalTypes/'
    for filename in os.listdir(dataFolder):
        if filename.endswith('.csv'):
            component = os.path.splitext(filename)[0]
            if component in vert_names:
                with open(dataFolder + filename, 'r', encoding='latin1') as csv:
                    # csv = unicode(csv, errors='ignore')
                    for i, l in enumerate(csv):
                        pass
                node_count[component] = i
                tot_nodes += i
            elif component in edge_names:
                with open(dataFolder + filename, 'r', encoding='latin1') as csv:
                    for i, l in enumerate(csv):
                        pass
                edge_count[component] = i
                tot_edges += i
    mdOut.write('## Node and Edge Count\n')
    mdOut.write('<table>\n')
    mdOut.write(f'<tr><th>Total Nodes: {tot_nodes}</th><th>Total Edges: {tot_edges}</th></tr>{nl}')
    mdOut.write('<tr><td>\n\n')
    mdOut.write('|Node|Count|\n')
    mdOut.write('|--|--|\n')
    for node in node_count:
        mdOut.write(f'|{node}|{node_count[node]}|{nl}')
    mdOut.write('</td><td>\n\n')
    mdOut.write('|Edge|Count|\n')
    mdOut.write('|--|--|\n')
    for edge in edge_count:
        mdOut.write(f'|{edge}|{edge_count[edge]}|{nl}')
    mdOut.write('</td></tr> </table>\n')

def add_title(project_title, mdOut):
    mdOut.write(f'# {project_title}{nl}')

def process_queries(mdOut):
    for file in glob.glob(input_folder + 'DBImportExport*.gsql'):
        mdOut.write('## Queries\n')
        with open(file, 'r') as query_file:
            queries = query_file.read()
            queries = re.findall(r'((?:CREATE|create)\s*\w*\s*(?:QUERY|query).*?\})\n', queries, re.S)
            for query in queries:
                query_line = re.search(r'(?:CREATE|create)\s*\w*\s*(?:QUERY|query)\s*(.*?)\s*\((.*?)\).*?(?:FOR|for)', query, re.S)
                query_name = query_line[1]
                mdOut.write(f'### {query_name}{nl}')

                query_attrs = query_line[2]
                if query_attrs != "":
                    mdOut.write('#### **Input Variables**\n')
                    mdOut.write('|Variable Name|Type|\n')
                    mdOut.write('|--|--|\n')
                    query_attrs = query_attrs.split(',')
                    for attr in query_attrs:
                        attr = attr.split(' ')
                        mdOut.write(f'|{attr[1]}|{attr[0]}|{nl}')
                mdOut.write('```\n')
                mdOut.write(query)
                mdOut.write('\n```\n')
                mdOut.write('---\n\n')

all_folders = glob.glob('./*/')
for folder in all_folders:
    input_folder = folder
    project_name = folder.split('/')[1]
    print(project_name)
    mdOut = open(input_folder + project_name + ".md", 'w')

    parse_schema()
    add_title(project_name, mdOut)
    generate_graph(input_folder, mdOut)
    get_folder_size(input_folder, mdOut)
    count_graph(input_folder, mdOut)
    add_schema_tables(mdOut)
    process_queries(mdOut)

    mdOut.close()
    break