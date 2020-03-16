#!/usr/bin/env python3
import os
import re
import shutil


with open('output/reviews.gexf', encoding='utf-8') as gexf_file:
    gexf = gexf_file.read()
gexf = re.sub(r"\s+", " ", gexf).strip()
with open('graphs/graph_template.html', encoding='utf-8') as template_file:
    template = template_file.read()
html = template.replace('<?gefx template="true"?>', gexf)
with open('output/graph.html', 'w', encoding='utf-8') as html_file:
    html_file.write(html)
if os.path.exists('output/js'):
    shutil.rmtree('output/js')
shutil.copytree('graphs/js', 'output/js')
