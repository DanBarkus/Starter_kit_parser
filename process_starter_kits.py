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
    schema_file = open(input_folder + 'db_scripts/schemas/schema.gsql', 'r') # Reading schema file

    for line in schema_file:
        # see if our line is dealing with a vertex or edge
        vertex_name = re.search(r'(?:create|CREATE|add|ADD) (?:vertex|VERTEX) ([\w\_\-]*)', line)
        edge_name = re.search(r'(?:create|CREATE|add|ADD) (\w*) (?:edge|EDGE) ([\w\_\-]*)', line)
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

def process_queries(mdOut):
    for file in glob.glob(input_folder + 'db_scripts/queries/*.gsql'): # Iterates towards queries
        mdOut.write('## Queries\n')
        with open(file, 'r') as query_file:
            queries = query_file.read()
            # queries = re.findall(r'((?:CREATE|create)\s*\w*\s*(?:QUERY|query).*?\})\n', queries, re.S)
            query_line = re.search(r'(?:CREATE|create)\s*\w*\s*(?:QUERY|query)\s*(.*?)\s*\((.*?)\).*?(?:FOR|for)', queries, re.S)
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
            # mdOut.write(str(query))
            mdOut.write('\n```\n')
            mdOut.write('---\n\n')

def get_loading_jobs():
    for file in glob.glob(input_folder + 'db_scripts/jobs/*.gsql'):
        print(file)

def get_data():
    for file in glob.glob(input_folder + 'data/*.*'):
        print(file)

def get_udfs():
    for file in glob.glob(input_folder + 'db_scripts/UDFs/*.hpp'):
        print(file)

# all_folders = glob.glob('./*/')
# for folder in all_folders:
#     input_folder = folder
#     project_name = folder.split('/')[1]
#     print(project_name)
#     mdOut = open(input_folder + project_name + ".md", 'w')
#     parse_schema()
#     process_queries(mdOut)

#     mdOut.close()
#     break

# parse_schema()
# process_queries(open("README.md", 'w'))
# get_loading_jobs()
# get_data()
# get_udfs()