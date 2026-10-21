"""Turn the design doc HTML into a flat list of blocks for build.js.

Usage: python3 extract.py ../RonDB-Group-Rate-Limits.html blocks.json

Blocks (JSON):
  {"t":"heading","level":1|2|3,"runs":[...]}
  {"t":"para","style":"normal|lead|meta|note|example","runs":[...]}
  {"t":"list","ordered":bool,"level":0|1,"style":"normal|note|example","runs":[...]}   # one item per block
  {"t":"code","text":"...","style":...}
  {"t":"table","rows":[[ {"runs":[...],"header":bool}, ...], ...],"header_rows":1,"caption":[...]|None}
  {"t":"image","src":"...","width":px,"height":px,"caption":[...]}
Runs: {"text":str,"code":bool,"bold":bool,"italic":bool}

Standard library only (no bs4/lxml on the build box).
"""
import json, struct, sys, os
from html.parser import HTMLParser

SRC = sys.argv[1]
OUT = sys.argv[2]
BASE = os.path.dirname(os.path.abspath(SRC))

def png_size(path):
    with open(path, 'rb') as f:
        head = f.read(24)
    w, h = struct.unpack('>II', head[16:24])
    return w, h

class Node:
    def __init__(self, tag, attrs, parent):
        self.tag = tag; self.attrs = dict(attrs); self.parent = parent; self.children = []
    def cls(self):
        return self.attrs.get('class', '').split()

class TreeBuilder(HTMLParser):
    VOID = {'img', 'br', 'meta', 'link', 'hr'}
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.root = Node('root', [], None); self.cur = self.root
    def handle_starttag(self, tag, attrs):
        n = Node(tag, attrs, self.cur); self.cur.children.append(n)
        if tag not in self.VOID: self.cur = n
    def handle_startendtag(self, tag, attrs):
        self.cur.children.append(Node(tag, attrs, self.cur))
    def handle_endtag(self, tag):
        n = self.cur
        while n is not None and n.tag != tag: n = n.parent
        if n is not None and n.parent is not None: self.cur = n.parent
    def handle_data(self, data):
        self.cur.children.append(data)

def find(node, tag):
    for c in node.children:
        if isinstance(c, Node):
            if c.tag == tag: return c
            r = find(c, tag)
            if r: return r
    return None

def runs_of(node, code=False, bold=False, italic=False, out=None, keep_ws=False):
    """Inline content -> runs. Collapses whitespace unless keep_ws, but keeps one
    space where the source had leading/trailing whitespace next to an inline tag."""
    if out is None: out = []
    for c in node.children:
        if isinstance(c, str):
            if keep_ws:
                text = c
            elif c.strip():
                text = ' '.join(c.split())
                if c[0].isspace(): text = ' ' + text
                if c[-1].isspace(): text = text + ' '
            else:
                text = ' ' if c else ''
            if text and out and out[-1]['text'].endswith(' ') and text.startswith(' '):
                text = text[1:]
            if text:
                if out and out[-1]['code'] == code and out[-1]['bold'] == bold and out[-1]['italic'] == italic:
                    out[-1]['text'] += text
                else:
                    out.append({'text': text, 'code': code, 'bold': bold, 'italic': italic})
        elif c.tag == 'br':
            out.append({'text': '\n', 'code': False, 'bold': False, 'italic': False})
        elif c.tag == 'code':
            runs_of(c, True, bold, italic, out, keep_ws)
        elif c.tag in ('strong', 'b'):
            runs_of(c, code, True, italic, out, keep_ws)
        elif c.tag in ('em', 'i'):
            runs_of(c, code, bold, True, out, keep_ws)
        elif c.tag == 'span' and 'meta' in c.cls():
            runs_of(c, code, bold, True, out, keep_ws)
        else:
            runs_of(c, code, bold, italic, out, keep_ws)
    return out

def tidy(runs):
    if runs:
        runs[0]['text'] = runs[0]['text'].lstrip()
        runs[-1]['text'] = runs[-1]['text'].rstrip()
    return [r for r in runs if r['text']]

def text_of_pre(pre):
    return ''.join(r['text'] for r in runs_of(pre, keep_ws=True)).strip('\n')

blocks = []

def emit_para(node, style='normal'):
    blocks.append({'t': 'para', 'style': style, 'runs': tidy(runs_of(node))})

def emit_list(node, ordered, style, level=0):
    for li in node.children:
        if not isinstance(li, Node) or li.tag != 'li': continue
        inline = Node('span', [], li); tail = []
        for c in li.children:
            if isinstance(c, Node) and c.tag in ('ul', 'ol', 'pre'):
                tail.append(c)
            else:
                inline.children.append(c)
        blocks.append({'t': 'list', 'ordered': ordered, 'level': level, 'style': style, 'runs': tidy(runs_of(inline))})
        for c in tail:
            if c.tag == 'pre':
                blocks.append({'t': 'code', 'text': text_of_pre(c), 'style': style})
            else:
                emit_list(c, c.tag == 'ol', style, level + 1)

def emit_table(div):
    tbl = find(div, 'table')
    cap = find(tbl, 'caption')
    rows = []; header_rows = 0
    for sec in tbl.children:
        if not isinstance(sec, Node): continue
        if sec.tag in ('thead', 'tbody'):
            for tr in sec.children:
                if isinstance(tr, Node) and tr.tag == 'tr':
                    cells = []
                    for td in tr.children:
                        if isinstance(td, Node) and td.tag in ('td', 'th'):
                            cells.append({'runs': tidy(runs_of(td)), 'header': td.tag == 'th'})
                    rows.append(cells)
                    if sec.tag == 'thead': header_rows += 1
    blocks.append({'t': 'table', 'rows': rows, 'header_rows': header_rows,
                   'caption': tidy(runs_of(cap)) if cap else None})

def emit_figure(fig):
    img = find(fig, 'img'); cap = find(fig, 'figcaption')
    if img is not None:
        path = os.path.join(BASE, img.attrs['src'])
        w, h = png_size(path)
        blocks.append({'t': 'image', 'src': path, 'width': w, 'height': h,
                       'caption': tidy(runs_of(cap)) if cap else None})
    else:
        for c in fig.children:
            if isinstance(c, Node): walk(c)

def walk(node, style='normal'):
    for c in node.children:
        if not isinstance(c, Node): continue
        t = c.tag
        if t in ('h1', 'h2', 'h3'):
            blocks.append({'t': 'heading', 'level': int(t[1]), 'runs': tidy(runs_of(c))})
        elif t == 'p':
            cl = c.cls()
            if 'eyebrow' in cl: continue
            st = 'lead' if 'lead' in cl else 'meta' if 'meta' in cl else style
            emit_para(c, st)
        elif t in ('ul', 'ol'):
            emit_list(c, t == 'ol', style)
        elif t == 'pre':
            blocks.append({'t': 'code', 'text': text_of_pre(c), 'style': style})
        elif t == 'div' and 'tablewrap' in c.cls():
            emit_table(c)
        elif t == 'div' and ('note' in c.cls() or 'example' in c.cls()):
            walk(c, 'note' if 'note' in c.cls() else 'example')
        elif t == 'figure':
            emit_figure(c)
        elif t == 'dl':
            for d in c.children:
                if isinstance(d, Node) and d.tag == 'dt':
                    emit_para(d, style)
                elif isinstance(d, Node) and d.tag == 'dd':
                    emit_para(d, style)
        elif t == 'nav':
            continue
        elif t in ('section', 'header', 'footer', 'main', 'body', 'html', 'div'):
            walk(c, style)

html = open(SRC, encoding='utf-8').read()
tb = TreeBuilder(); tb.feed(html)
body = find(tb.root, 'body')
walk(body)
json.dump(blocks, open(OUT, 'w'), ensure_ascii=False, indent=1)
print(len(blocks), 'blocks;', sum(1 for b in blocks if b['t'] == 'image'), 'images;', sum(1 for b in blocks if b['t'] == 'table'), 'tables')
