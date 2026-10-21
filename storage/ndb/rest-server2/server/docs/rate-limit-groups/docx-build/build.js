// Build the Word version of the design doc from blocks.json (see extract.py).
//
//   npm install --cache ./.npmcache --no-audit --no-fund      (sandbox blocks ~/.npm)
//   python3 extract.py ../RonDB-Group-Rate-Limits.html blocks.json
//   node build.js blocks.json ./RonDB-Group-Rate-Limits.docx
//
// Why not `textutil -convert docx`: it silently drops every image.
const fs = require('fs');
const {
  Document, Packer, Paragraph, TextRun, ImageRun, Table, TableRow, TableCell,
  HeadingLevel, AlignmentType, WidthType, ShadingType, BorderStyle, LevelFormat,
  TableOfContents, Footer, PageNumber, TableLayoutType, VerticalAlign,
} = require('docx');

const blocks = JSON.parse(fs.readFileSync(process.argv[2], 'utf8'));
const OUT = process.argv[3];
const DOC_TITLE = process.argv[4] || 'Rate-limit buckets: shared database budgets for teams';
const FOOTER_TEXT = process.argv[5] || 'RonDB rate-limit buckets · design proposal · ';

// A4 with 2 cm margins.
const PAGE_W = 11906, PAGE_H = 16838, MARGIN = 1134;
const CONTENT_W = PAGE_W - 2 * MARGIN;                 // DXA
const CONTENT_PX = Math.round(CONTENT_W / 1440 * 96);  // px at 96 dpi for ImageRun

const FONT = 'Calibri';
const MONO = 'Consolas';
const ACCENT = '096B67', MUTED = '566574', LINE = 'D7E0E5', SOFT = 'EDF4F4', CODEBG = 'EEF1F4', NOTE = 'FFF7E7', NOTELINE = 'B67A12';

// ---- inline runs -----------------------------------------------------------
function runsToTextRuns(runs, opts = {}) {
  const out = [];
  for (const r of runs) {
    const parts = r.text.split('\n');
    parts.forEach((part, i) => {
      if (i > 0) out.push(new TextRun({ break: 1 }));
      if (!part) return;
      out.push(new TextRun({
        text: part,
        bold: r.bold || opts.bold || undefined,
        italics: r.italic || opts.italic || undefined,
        font: r.code ? MONO : (opts.font || FONT),
        size: r.code ? (opts.size ? opts.size - 1 : 20) : (opts.size || 21),
        color: opts.color || undefined,
        shading: r.code ? { type: ShadingType.CLEAR, fill: CODEBG, color: 'auto' } : undefined,
      }));
    });
  }
  return out;
}

// ---- numbering ---------------------------------------------------------------
// bullets: one shared reference; ordered lists: a fresh reference per <ol> so numbering restarts.
const numberingConfig = [{
  reference: 'bullets',
  levels: [0, 1].map(l => ({
    level: l, format: LevelFormat.BULLET, text: l === 0 ? '•' : '–', alignment: AlignmentType.LEFT,
    style: { paragraph: { indent: { left: 540 + l * 420, hanging: 300 } } },
  })),
}];
let olCounter = 0;
function newOrderedRef() {
  const ref = 'ol' + (++olCounter);
  numberingConfig.push({
    reference: ref,
    levels: [0, 1].map(l => ({
      level: l, format: LevelFormat.DECIMAL, text: '%' + (l + 1) + '.', alignment: AlignmentType.LEFT,
      style: { paragraph: { indent: { left: 540 + l * 420, hanging: 300 } } },
    })),
  });
  return ref;
}

// ---- block renderers -----------------------------------------------------------
function heading(b) {
  const lvl = b.level === 1 ? HeadingLevel.TITLE : b.level === 2 ? HeadingLevel.HEADING_1 : HeadingLevel.HEADING_2;
  return new Paragraph({ heading: lvl, children: runsToTextRuns(b.runs, { size: b.level === 1 ? 40 : b.level === 2 ? 30 : 24, bold: true, color: b.level === 1 ? ACCENT : undefined }) });
}

function para(b, inBox) {
  const st = b.style;
  if (st === 'lead') return new Paragraph({ spacing: { after: 200, line: 300 }, children: runsToTextRuns(b.runs, { size: 24 }) });
  if (st === 'meta') return new Paragraph({ spacing: { after: 160 }, children: runsToTextRuns(b.runs, { size: 18, color: MUTED, italic: true }) });
  return new Paragraph({ spacing: { after: inBox ? 100 : 160, line: 276 }, children: runsToTextRuns(b.runs) });
}

function listItem(b, ref) {
  return new Paragraph({
    numbering: { reference: ref, level: b.level },
    spacing: { after: 80, line: 276 },
    children: runsToTextRuns(b.runs),
  });
}

function codeBlock(b) {
  const lines = b.text.split('\n');
  return lines.map((line, i) => new Paragraph({
    shading: { type: ShadingType.CLEAR, fill: CODEBG, color: 'auto' },
    spacing: { before: i === 0 ? 120 : 0, after: i === lines.length - 1 ? 160 : 0, line: 240 },
    indent: { left: 200, right: 200 },
    border: i === 0 ? { top: { style: BorderStyle.SINGLE, size: 4, color: LINE } } : i === lines.length - 1 ? { bottom: { style: BorderStyle.SINGLE, size: 4, color: LINE } } : undefined,
    children: [new TextRun({ text: line || ' ', font: MONO, size: 17 })],
  }));
}

function caption(runs) {
  return new Paragraph({ spacing: { before: 60, after: 240 }, children: runsToTextRuns(runs, { size: 18, color: MUTED, italic: true }) });
}

function image(b) {
  const w = Math.min(CONTENT_PX, b.width);
  const h = Math.round(b.height * (w / b.width));
  const out = [new Paragraph({
    spacing: { before: 120, after: 40 }, keepNext: true,
    children: [new ImageRun({ type: 'png', data: fs.readFileSync(b.src), transformation: { width: w, height: h } })],
  })];
  if (b.caption) out.push(caption(b.caption));
  return out;
}

const thinBorder = { style: BorderStyle.SINGLE, size: 4, color: LINE };
const cellBorders = { top: thinBorder, bottom: thinBorder, left: thinBorder, right: thinBorder };

function table(b) {
  const ncols = Math.max(...b.rows.map(r => r.length));
  // column widths proportional to the longest text in each column, clamped.
  const lens = Array(ncols).fill(0);
  for (const row of b.rows) row.forEach((c, i) => {
    const t = c.runs.map(r => r.text).join('');
    const longest = Math.max(...t.split('\n').map(s => s.length), 1);
    lens[i] = Math.max(lens[i], Math.min(longest, 60));
  });
  const total = lens.reduce((a, c) => a + c, 0);
  let widths = lens.map(l => Math.max(1100, Math.round(CONTENT_W * l / total)));
  const sum = widths.reduce((a, c) => a + c, 0);
  widths = widths.map(w => Math.round(w * CONTENT_W / sum));
  widths[ncols - 1] += CONTENT_W - widths.reduce((a, c) => a + c, 0);

  const rows = b.rows.map((row, ri) => new TableRow({
    tableHeader: ri < b.header_rows,
    cantSplit: true,
    children: row.map((c, ci) => new TableCell({
      width: { size: widths[ci], type: WidthType.DXA },
      borders: cellBorders,
      shading: c.header ? { type: ShadingType.CLEAR, fill: SOFT, color: 'auto' } : undefined,
      margins: { top: 80, bottom: 80, left: 100, right: 100 },
      verticalAlign: VerticalAlign.TOP,
      children: [new Paragraph({ spacing: { after: 0, line: 260 }, children: runsToTextRuns(c.runs, { size: 19, bold: c.header || undefined }) })],
    })),
  }));
  const out = [];
  if (b.caption) out.push(new Paragraph({ spacing: { before: 120, after: 60 }, keepNext: true, children: runsToTextRuns(b.caption, { size: 18, color: MUTED, italic: true }) }));
  out.push(new Table({ width: { size: CONTENT_W, type: WidthType.DXA }, columnWidths: widths, layout: TableLayoutType.FIXED, rows }));
  out.push(new Paragraph({ spacing: { after: 120 }, children: [] }));
  return out;
}

// A note/example box: one-cell table with a tinted background and a coloured left border.
function box(children, kind) {
  const fill = kind === 'note' ? NOTE : SOFT;
  const left = { style: BorderStyle.SINGLE, size: 24, color: kind === 'note' ? NOTELINE : ACCENT };
  const none = { style: BorderStyle.NONE, size: 0, color: 'FFFFFF' };
  return [
    new Table({
      width: { size: CONTENT_W, type: WidthType.DXA }, columnWidths: [CONTENT_W], layout: TableLayoutType.FIXED,
      rows: [new TableRow({ children: [new TableCell({
        width: { size: CONTENT_W, type: WidthType.DXA },
        shading: { type: ShadingType.CLEAR, fill, color: 'auto' },
        borders: { top: none, bottom: none, right: none, left },
        margins: { top: 140, bottom: 60, left: 220, right: 220 },
        children,
      })] })],
    }),
    new Paragraph({ spacing: { after: 160 }, children: [] }),
  ];
}

// ---- assemble ----------------------------------------------------------------
const body = [];
let i = 0;
let currentOlRef = null, lastWasOrdered = false;
while (i < blocks.length) {
  const b = blocks[i];
  const boxed = b.style === 'note' || b.style === 'example';
  if (boxed) {
    const kind = b.style; const inner = [];
    let ol = null, prevOrdered = false;
    while (i < blocks.length && blocks[i].style === kind && ['para', 'list', 'code'].includes(blocks[i].t)) {
      const x = blocks[i];
      if (x.t === 'para') inner.push(para(x, true));
      else if (x.t === 'code') inner.push(...codeBlock(x));
      else {
        if (x.ordered) { ol = ol || newOrderedRef(); inner.push(listItem(x, ol)); }
        else inner.push(listItem(x, 'bullets'));
        prevOrdered = x.ordered;
      }
      i++;
    }
    body.push(...box(inner, kind));
    continue;
  }
  switch (b.t) {
    case 'heading': body.push(heading(b)); lastWasOrdered = false; break;
    case 'para': body.push(para(b, false)); lastWasOrdered = false; break;
    case 'list':
      if (b.ordered) {
        if (!lastWasOrdered) currentOlRef = newOrderedRef();
        body.push(listItem(b, currentOlRef)); lastWasOrdered = true;
      } else { body.push(listItem(b, 'bullets')); lastWasOrdered = b.level > 0 ? lastWasOrdered : false; }
      break;
    case 'code': body.push(...codeBlock(b)); lastWasOrdered = false; break;
    case 'table': body.push(...table(b)); lastWasOrdered = false; break;
    case 'image': body.push(...image(b)); lastWasOrdered = false; break;
  }
  i++;
}

// Table of contents after the header block, i.e. before the first level-2 heading; header blocks map 1:1 onto body.
const tocIdx = blocks.findIndex(b => b.t === 'heading' && b.level === 2);
body.splice(tocIdx, 0,
  new Paragraph({ heading: HeadingLevel.HEADING_1, children: [new TextRun({ text: 'Contents', bold: true, size: 30 })] }),
  new TableOfContents('Contents', { hyperlink: true, headingStyleRange: '1-2' }),
  new Paragraph({ spacing: { after: 200 }, children: [] }),
);

const doc = new Document({
  creator: 'RonDB team',
  title: DOC_TITLE,
  features: { updateFields: true },
  styles: {
    default: { document: { run: { font: FONT, size: 21 } } },
    paragraphStyles: [
      { id: 'Title', name: 'Title', basedOn: 'Normal', next: 'Normal', run: { size: 40, bold: true, color: ACCENT, font: FONT }, paragraph: { spacing: { after: 240 } } },
      { id: 'Heading1', name: 'Heading 1', basedOn: 'Normal', next: 'Normal', quickFormat: true, run: { size: 30, bold: true, color: '202A33', font: FONT }, paragraph: { spacing: { before: 420, after: 160 }, outlineLevel: 0, keepNext: true } },
      { id: 'Heading2', name: 'Heading 2', basedOn: 'Normal', next: 'Normal', quickFormat: true, run: { size: 24, bold: true, color: '202A33', font: FONT }, paragraph: { spacing: { before: 280, after: 100 }, outlineLevel: 1, keepNext: true } },
    ],
  },
  numbering: { config: numberingConfig },
  sections: [{
    properties: { page: { size: { width: PAGE_W, height: PAGE_H }, margin: { top: MARGIN, bottom: MARGIN, left: MARGIN, right: MARGIN } } },
    footers: { default: new Footer({ children: [new Paragraph({ alignment: AlignmentType.CENTER, children: [
      new TextRun({ text: FOOTER_TEXT, size: 16, color: MUTED }),
      new TextRun({ children: [PageNumber.CURRENT], size: 16, color: MUTED }),
    ] })] }) },
    children: body,
  }],
});

Packer.toBuffer(doc).then(buf => { fs.writeFileSync(OUT, buf); console.log('wrote', OUT, buf.length, 'bytes'); });
