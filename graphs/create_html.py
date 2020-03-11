#!/usr/bin/env python3
import re
import shutil


with open('output/reviews.gexf') as gexf_file:
    gexf = gexf_file.read()
gexf = re.sub(r"\s+", " ", gexf).strip()
with open('graphs/graph_template.html') as template_file:
    template = template_file.read()
html = template.replace('<?gefx template="true"?>', gexf)
with open('output/graph.html', 'w') as html_file:
    html_file.write(html)
shutil.rmtree('output/js')
shutil.copytree('graphs/js', 'output/js')
