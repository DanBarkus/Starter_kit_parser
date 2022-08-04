import re
import os
import math
import glob
import casefy

# Newline variable for f-strings
nl = '\n'

input_folder = './Supply-Chain-Analysis/'
project_name = ''

graph_name = ''
verts = []
edges = []
vert_names = []
edge_names = []
schema = {}
# this will hold the replacement mapping of old style words to new style
# example: {graphname: Graph_Name}
replacement_dict = {}

capital_words = ["ACCUM","ADD","ADMIN","ALL","ALLOCATE","ALTER","AND","ANY","AS","ASC",
                 "AVG","BAG","BATCH","BETWEEN","BIGINT","BLOB","BOOL","BOOLEAN","BOTH",
                 "BREAK","BY","CALL","CASCADE","CASE","CATCH","CHAR","CHARACTER","CHECK",
                 "CLOB","COALESCE","COMPRESS","CONST","CONSTRAINT","CONTINUE","COUNT",
                 "CREATE","CURRENT_DATE","CURRENT_TIME","CURRENT_TIMESTAMP","CURSOR",
                 "DATA_SOURCE","DATETIME","DATETIME_ADD","DATETIME_SUB","DECIMAL",
                 "DECLARE","DEFAULT","DELETE","DESC","DISTRIBUTED","DO","DOUBLE","DROP",
                 "EDGE","ELSE","ELSEIF","END","ESCAPE","EXCEPTION","EXISTS",
                 "EXPRFUNCTIONS","EXPRUTIL","FALSE","FILE","FILENAME","FILTER",
                 "FIXED_BINARY","FLATTEN_JSON_ARRAY","FLOAT","FOR","FOREACH","FROM",
                 "GLOBAL","GRAPH","GROUP","GROUPBYACCUM","HAVING","HEADER","HEAPACCUM",
                 "IF","IGNORE","IN","INDEX","INPUT_LINE_FILTER","INSERT","INT","INT16",
                 "INT32","INT32_T","INT64_T","INT8","INTEGER","INTERPRET","INTERSECT",
                 "INTO","IS","ISEMPTY","JOB","JOIN","JSONARRAY","JSONOBJECT","KAFKA",
                 "KEY","LEADING","LIKE","LIMIT","LIST","LOAD","LOADACCUM","LOG","LONG",
                 "MAP","NOT","NOW","NULL","OFFSET","ON","OR","ORDER","PINNED","POST_ACCUM",
                 "PRIMARY","PRIMARY_ID","PRINT","PROXY","QUERY","QUIT","RAISE","RANGE",
                 "REDUCE","REPLACE","RETURN","RETURNS","S3","SAMPLE","SELECT",
                 "SELECTVERTEX","SET","STATIC","STRING","SUM","TARGET","TEMP_TABLE",
                 "THEN","TO","TO_CSV","TO_DATETIME","TO_FLOAT","TO_INT","TOKEN","TOKEN_LEN",
                 "TOKENBANK","TRAILING","TRIM","TRUE","TRY","TUPLE","TYPEDEF","UINT",
                 "UINT16","UINT32","UINT32_T","UINT64_T","UINT8","UINT8_T","UNION","UPDATE",
                 "UPSERT","USING","VALUES","VERTEX","WHEN","WHERE","WHILE"]

def compile_list_to_regex(inList):
    in_list_length = len(inList)
    re_list = "r'"
    for idx, item in enumerate(inList):
        re_list += item 
        if idx < in_list_length -1:
            re_list += '\\b|'
    re_list += "\\b'"
    return re_list

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
    graphs = []
    schema_file = open(input_folder + 'db_scripts/schemas/schema.gsql', 'r') # Reading schema file

    for line in schema_file:
        # see if our line is dealing with a vertex or edge or graph name
        graph_name = re.search(r'graph\W*(\w+)\W*(\(\)|\{)', line, flags=re.IGNORECASE)
        vertex_name = re.search(r'(?:create|add) (?:vertex) ([\w\_\-]*)', line, flags=re.IGNORECASE)
        edge_name = re.search(r'(?:create|add) (\w*) (?:edge) ([\w\_\-]*)', line, flags=re.IGNORECASE)

        if graph_name != None:
            graph_name = graph_name.group(1)
            graphs.append(graph_name)
        
        elif vertex_name != None:
            vertex = {}
            vertex_name = vertex_name.group(1)
            vert_names.append(vertex_name)
            # we already have the name so we only need to deal with what comes after it
            details = re.search(r'\(.*$', line).group(0)
            primary_id = re.search(r'(?:PRIMARY_ID) ([\w\_\-]*) (\w*)', details, flags=re.IGNORECASE)
            vertex = {"name": vertex_name, "primary_id": {"name": primary_id.group(1), "type": primary_id.group(2)}}
            # get a list of attribute - type combos
            attributes = re.findall(r', ([\w\_\-]*) (\w*)', details)
            attrs = []
            for attribute in attributes:
                att = {"name": attribute[0], "type": attribute[1]}
                attrs.append(att)
            vertex["attributes"] = attrs
            # get everything after the closing ')'
            additional_details = re.search(r'\)\s?(?:with)(.*)', details, flags=re.IGNORECASE).group(1)
            additional_details = re.findall(r'(\w*)=\"([\w\_\-]*)\"', additional_details)
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
            if directed:
                reverse_edge = re.search(r'reverse_edge\W*\=\"(\w*)\"', line, flags=re.IGNORECASE).group(1)
                edge['directed'] = reverse_edge
            try:
                additional_details = re.search(r'\)\s?(?:with)(.*)', details, flags=re.IGNORECASE).group(1)
                additional_details = re.findall(r'(\w*)=\"([\w\_\-]*)\"', additional_details)
                for addl_deet in additional_details:
                    edge[addl_deet[0]] = addl_deet[1]
            except:
                pass
            edges.append(edge)

    schema = {"nodes": verts, "edges": edges, "graphs": graphs}
            
    schema_file.close()
    # print(schema)
    return schema

# populate the replacement dict with the new schema names
def correct_schema(in_schema):
    for graph in in_schema['graphs']:
        graph_name = convert_to_capital_snake(graph)
        replacement_dict[graph] = graph_name
    for vert in in_schema['nodes']:
        vert_name = convert_to_capital_snake(vert['name'])
        replacement_dict[vert['name']] = vert_name
        v_p_id = vert['primary_id']['name']
        v_p_id = casefy.snakecase(v_p_id)
        replacement_dict[vert['primary_id']['name']] = v_p_id
        for attribute in vert['attributes']:
            attr_name = attribute['name']
            attr_name = casefy.snakecase(attr_name)
            replacement_dict[attribute['name']] = attr_name
    for edge in in_schema['edges']:
        edge_name = convert_to_capital_snake(edge['name'])
        replacement_dict[edge['name']] = edge_name
        if edge['directed']:
            rev_edge_name = edge['directed']
            rev_edge_name = convert_to_capital_snake(rev_edge_name)
            replacement_dict[edge['directed']] = rev_edge_name
    # print(replacement_dict)

def parse_queries():
    for q_file in glob.glob(input_folder + 'db_scripts/queries/*.gsql'):
        query_file = open(q_file, 'r') # Reading schema file
        for line in query_file:

            accumulator_name = re.search(r'(\w+)accum.*?(@+)(\w+)', line, flags=re.IGNORECASE)
            query_name = re.search(r'create\W*query\W*(\w*?)\W*\(', line, flags=re.IGNORECASE)

            if accumulator_name != None:
                # type of the accumulator from the declaration
                accum_type = accumulator_name.group(1)
                # @ or @@ from the accumulator (they get stripped by the case conversion)
                accum_ats = accumulator_name.group(2)
                # accumulator name
                accum_name = accumulator_name.group(3)

                new_accum_name = accum_name

                # See if this is a collection accumulator (we need to add the collection type to the end of the accum name)
                if bool(re.match(r'(set|list|map|array|bag|heap)', accum_type, flags=re.IGNORECASE)):
                    # see if the accumulator name already has the type in it
                    name_type = re.search(r'(set|list|map|array|bag|heap)\b', accum_name, flags=re.IGNORECASE)
                    # Add the type to the end of the accumulator
                    if name_type == None:
                        new_accum_name = accum_name + accum_type
                    new_accum_name = casefy.snakecase(new_accum_name)
                    new_accum_name = accum_ats + new_accum_name
                    replacement_dict[accum_ats + accum_name] = new_accum_name
            
            elif query_name != None:
                query_name = query_name.group(1)
                new_query_name = casefy.snakecase(query_name)
                if query_name != new_query_name:
                    replacement_dict[query_name] = new_query_name

        
    print(replacement_dict)

def get_replacement(in_word):
    return replacement_dict[in_word[0]]

def do_replacement():
    to_replace = replacement_dict.keys()
    to_replace_regex = compile_list_to_regex(to_replace)
    print(to_replace_regex)
    for q_file in glob.glob(input_folder + 'db_scripts/queries/*.gsql'):
        with open(q_file, 'r') as query_file:
            new_file = open(q_file+'.new', 'w+')
            for line in query_file:
                line = re.sub(to_replace_regex, get_replacement, line)
                new_file.write(line)

def convert_to_capital_snake(in_word):
    out_word = casefy.titlecase(in_word)
    out_word = out_word.replace(" ", "_")
    return out_word

schema = parse_schema()
correct_schema(schema)
parse_queries()
do_replacement()

def to_upper(to_replace):
    return casefy.uppercase(to_replace[0])

def caps_keywords():
    words_to_capital = compile_list_to_regex(capital_words)
    with open(input_folder + 'db_scripts/schemas/schema.gsql', 'r+') as schema_file:
        content = schema_file.read()
        cap_content = re.sub(words_to_capital, to_upper, content, flags=re.IGNORECASE)
        # print(cap_content)
        schema_file.write(cap_content)

# caps_keywords()

def get_loading_jobs():
    for file in glob.glob(input_folder + 'db_scripts/jobs/*.gsql'):
        print(file)

def get_data():
    for file in glob.glob(input_folder + 'data/*.*'):
        print(file)

def get_udfs():
    for file in glob.glob(input_folder + 'db_scripts/UDFs/*.hpp'):
        print(file)