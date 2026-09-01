"""Stijl voor de mockup, overgenomen van de referentiepagina.

Losse module en geen f-string: CSS staat vol accolades en dat leest niet meer
zodra je het in een format-string propt.
"""

FONT_LINK = ('<link rel="preconnect" href="https://fonts.googleapis.com">'
             '<link href="https://fonts.googleapis.com/css2?'
             'family=Manrope:wght@500;600;700;800&display=swap" rel="stylesheet">')

CSS = """
:root {
  --primary:#1f99c4; --header:#55aed1; --cta:#f37632; --cta-dark:#e85b0e;
  --magenta:#be4693; --green:#26a660; --ink:#272727; --body:#545454;
  --muted:#9a9a9a; --line:#e0e0e0; --bg:#fafafa; --white:#fff;
  --font:"Manrope",Arial,sans-serif; --wrap:68rem;
}
*,*::before,*::after { box-sizing:border-box; }
html { font-size:16px; scroll-behavior:smooth; }
body { margin:0; font-family:var(--font); font-weight:500; color:var(--body);
  background:var(--bg); -webkit-font-smoothing:antialiased; }
img { max-width:100%; height:auto; display:block; }
a { color:var(--primary); }
a:hover { color:var(--ink); }

/* mockup-balk (hoort niet bij het ontwerp, wel bij het beoordelen) */
.mockbar { background:var(--ink); color:#fff; font-size:.78rem; padding:.5rem 1.25rem;
  display:flex; gap:1rem; align-items:center; flex-wrap:wrap; }
.mockbar b { background:#ffd166; color:var(--ink); padding:.1rem .5rem; border-radius:.25rem;
  font-weight:800; letter-spacing:.06em; }
.mockbar code { color:#cfe9f4; }
.mockbar .sp { margin-left:auto; }
.mockbar button { background:#3d3d3d; color:#fff; border:1px solid #555; border-radius:.35rem;
  padding:.25rem .7rem; font:inherit; font-size:.78rem; cursor:pointer; }
.mockbar button:hover { background:#4d4d4d; }

.top { background:var(--header); color:var(--white); }
.top-inner { max-width:var(--wrap); margin:0 auto; padding:.85rem 1.25rem; display:flex;
  align-items:baseline; justify-content:space-between; gap:1rem; }
.wordmark { font-weight:800; font-size:1.35rem; letter-spacing:-.04em; color:var(--white);
  text-decoration:none; }
.wordmark span { color:var(--ink); }
.top-kicker { font-size:.8rem; font-weight:700; letter-spacing:.06em; text-transform:uppercase;
  opacity:.95; }

.page { max-width:var(--wrap); margin:0 auto; padding:2rem 1.25rem 7rem; }
.lead { max-width:44rem; margin-bottom:1.75rem; }
.lead h1 { margin:0 0 .6rem; color:var(--ink); font-size:clamp(1.8rem,4vw,2.6rem);
  font-weight:800; letter-spacing:-.035em; line-height:1.15; }
.lead h1 em { font-style:normal; color:var(--cta); }
.lead-do { font-weight:600; font-size:1.05rem; line-height:1.55; color:var(--body); margin:0 0 .7rem; }
.stamp { font-size:.82rem; color:var(--muted); margin:0 0 .6rem; }
.lead-why { color:var(--body); max-width:62ch; font-size:.95rem; margin:0; }

.chooser { margin:0 0 2rem; padding:1rem 1.1rem 1.15rem; background:var(--white);
  border:1px solid var(--line); border-radius:.75rem; }
.chooser p { margin:0 0 .7rem; font-size:.8rem; font-weight:800; letter-spacing:.08em;
  text-transform:uppercase; color:var(--muted); }
.chips { display:flex; flex-wrap:wrap; gap:.5rem; }
.chip { display:inline-flex; align-items:center; padding:.55rem .9rem; border:1px solid var(--line);
  border-radius:999px; background:var(--white); color:var(--ink); text-decoration:none;
  font-size:.92rem; font-weight:700; }
.chip:hover { border-color:var(--primary); color:var(--primary); }
.chip.is-on { background:var(--primary); border-color:var(--primary); color:var(--white); }

.funnel { display:grid; gap:1.25rem; }
.hero { background:var(--white); border:1px solid var(--line); border-left:10px solid var(--primary);
  border-radius:.75rem; overflow:hidden; }
.hero-band { background:var(--primary); color:var(--white); padding:.7rem 1.25rem; display:flex;
  align-items:baseline; justify-content:space-between; gap:1rem; flex-wrap:wrap; }
.hero-band strong { font-size:.8rem; letter-spacing:.1em; text-transform:uppercase; }
.hero-band span { font-size:.9rem; font-weight:600; }
.hero-grid { display:grid; grid-template-columns:minmax(12rem,38%) 1fr; gap:1.5rem;
  padding:1.5rem 1.4rem 1.6rem; align-items:center; }
.hero-grid img { width:100%; aspect-ratio:1; object-fit:contain; background:var(--bg);
  border-radius:.5rem; }
.rank-name { display:flex; align-items:baseline; gap:.75rem; margin-bottom:.4rem; }
.rank-name .n { font-size:2.4rem; font-weight:800; color:var(--primary); letter-spacing:-.05em;
  line-height:1; }
.rank-name h2 { margin:0; font-size:clamp(1.25rem,2.4vw,1.7rem); color:var(--ink); font-weight:800;
  letter-spacing:-.03em; line-height:1.2; }
.verdict { margin:0 0 .85rem; font-size:1.05rem; line-height:1.45; color:var(--ink); font-weight:600; }
.pluses { list-style:none; margin:0 0 .85rem; padding:0; }
.pluses li { display:flex; gap:.5rem; align-items:flex-start; margin:0 0 .3rem; color:var(--ink);
  font-weight:600; font-size:.95rem; }
.pluses li .tick { flex:none; width:1.1rem; height:1.1rem; margin-top:.12rem; color:var(--green); }
.letop { margin:0 0 1rem; font-size:.92rem; color:var(--magenta); font-weight:700; line-height:1.4; }
.letop .lbl { display:inline-block; margin-right:.35rem; padding:.1rem .4rem; border-radius:.25rem;
  background:var(--magenta); color:var(--white); font-size:.7rem; letter-spacing:.04em;
  text-transform:uppercase; font-weight:800; vertical-align:1px; }
.voorjou { margin:0 0 1.1rem; color:var(--body); font-size:.95rem; }
.buy { display:flex; flex-wrap:wrap; align-items:center; gap:.85rem 1.1rem; }
.price { font-size:1.7rem; font-weight:800; color:var(--ink); letter-spacing:-.03em;
  font-variant-numeric:tabular-nums; }
.btn { display:inline-flex; align-items:center; justify-content:center; min-height:2.75rem;
  padding:.55rem 1.15rem; background:var(--cta); color:var(--white); font-weight:800;
  font-size:.95rem; text-decoration:none; border-radius:.5rem; border:1px solid var(--cta); }
.btn:hover { background:var(--cta-dark); border-color:var(--cta-dark); color:var(--white); }
.btn-ghost { background:var(--white); color:var(--cta); border-color:var(--cta); }

.shops { display:flex; flex-direction:column; gap:.15rem; margin:.6rem 0 0; }
.shop { display:grid; grid-template-columns:1fr auto; align-items:center; gap:.75rem;
  padding:.45rem .7rem; border-radius:.5rem; }
.shop-name { font-size:.95rem; color:var(--ink); min-width:0; }
.shop-price { font-size:.95rem; font-weight:700; color:var(--ink);
  font-variant-numeric:tabular-nums; white-space:nowrap; }
.shop.best { background:#fdf2fa; }
.shop.best .shop-name { font-weight:600; }
.shop.best .shop-price { font-size:1.15rem; color:var(--magenta); }
.shop.best .shop-name::after { content:"beste prijs"; margin-left:.5rem; font-size:.7rem;
  font-weight:700; letter-spacing:.04em; text-transform:uppercase; color:var(--magenta);
  background:var(--white); border:1px solid var(--magenta); border-radius:999px;
  padding:.1rem .45rem; white-space:nowrap; }
.buy.buy-shops { flex-direction:column; align-items:stretch; gap:0; }
.alt .buy.buy-shops { grid-column:1 / -1; }
.buy.buy-shops .btn, .buy.buy-shops .btn-ghost { align-self:start; margin-top:.7rem; }

.alts-head { margin:.5rem 0 0; padding-left:.2rem; }
.alts-head h2 { margin:0 0 .25rem; font-size:1.15rem; color:var(--ink); font-weight:800; }
.alts-head p { margin:0 0 .25rem; color:var(--body); font-size:.95rem; }
.alts { display:grid; grid-template-columns:1fr 1fr; gap:1rem; }
.alt { background:var(--white); border:1px solid var(--line); border-left:5px solid var(--cta);
  border-radius:.75rem; padding:1.1rem 1.15rem 1.2rem; display:grid;
  grid-template-columns:6.5rem 1fr; gap:.9rem 1rem; align-items:start; }
.alt.alt-b { border-left-color:var(--magenta); }
.alt img { width:6.5rem; height:6.5rem; object-fit:contain; background:var(--bg);
  border-radius:.4rem; grid-row:span 6; }
.why { display:inline-block; width:max-content; max-width:100%; background:var(--cta);
  color:var(--white); font-size:.72rem; font-weight:800; letter-spacing:.04em;
  text-transform:uppercase; padding:.22rem .55rem; border-radius:999px; margin-bottom:.35rem; }
.alt-b .why { background:var(--magenta); }
.alt h3 { margin:0 0 .15rem; font-size:1.05rem; color:var(--ink); font-weight:800;
  letter-spacing:-.02em; }
.alt .n { font-weight:800; color:var(--muted); font-size:.85rem; margin-right:.25rem; }
.alt .verdict { font-size:.92rem; font-weight:600; margin-bottom:.5rem; }
.alt .pluses li { font-size:.88rem; }
.alt .letop { margin-bottom:.65rem; font-size:.85rem; }
.alt .voorjou { font-size:.85rem; margin-bottom:.7rem; }
.alt .price { font-size:1.25rem; }

.rest-head { margin-top:.4rem; }
.rest-head h2 { margin:0 0 .2rem; font-size:1.15rem; color:var(--ink); font-weight:800; }
.rest-head p { margin:0 0 .4rem; font-size:.95rem; }
.rest { background:var(--white); border:1px solid var(--line); border-radius:.75rem;
  overflow:hidden; }
.row { display:grid; grid-template-columns:2rem 4.25rem 1fr auto; gap:.75rem .9rem;
  align-items:center; padding:.85rem 1rem; border-top:1px solid var(--line); }
.row:first-child { border-top:0; }
.row .n { font-weight:800; color:var(--muted); font-variant-numeric:tabular-nums; text-align:right; }
.row img { width:4.25rem; height:4.25rem; object-fit:contain; background:var(--bg);
  border-radius:.35rem; }
.row h3 { margin:0 0 .15rem; font-size:.95rem; color:var(--ink); font-weight:800; }
.row .voorjou { margin:0 0 .25rem; font-size:.82rem; color:var(--body); }
.row .letop { margin:0; font-size:.8rem; font-weight:700; }
.row .buy { flex-direction:column; align-items:flex-end; gap:.35rem; text-align:right; }
.row .price { font-size:1.05rem; }
.row .link { font-size:.82rem; font-weight:800; text-decoration:none; color:var(--primary);
  white-space:nowrap; }
.row:hover { background:var(--bg); }

.sect { margin:3rem 0 0; }
.sect > h2 { font-size:1.35rem; font-weight:800; letter-spacing:-.01em; margin:0 0 .35rem;
  color:var(--ink); }
.sect > p.intro { margin:0 0 1rem; color:var(--body); max-width:62ch; }
.card { background:var(--white); border:1px solid var(--line); border-radius:.9rem; padding:1.25rem;
  box-shadow:0 1px 4px rgba(0,0,0,.05); }
.cmp { overflow-x:auto; }
.cmp table { width:100%; border-collapse:collapse; font-size:.95rem; min-width:34rem; }
.cmp th, .cmp td { padding:.7rem .8rem; text-align:left; vertical-align:top;
  border-bottom:1px solid var(--line); }
.cmp th { font-size:.72rem; text-transform:uppercase; letter-spacing:.06em; color:var(--muted);
  font-weight:700; }
.cmp tr:last-child td { border-bottom:none; }
.cmp td.p { font-weight:800; white-space:nowrap; }
.cmp td.nm { font-weight:600; color:var(--ink); }
.faq details { border-bottom:1px solid var(--line); padding:.6rem 0; }
.faq details:last-child { border-bottom:0; }
.faq summary { cursor:pointer; font-weight:700; color:var(--ink); }
.faq p { margin:.5rem 0 0; color:var(--body); }
.bronnen { display:flex; gap:.4rem; flex-wrap:wrap; margin-top:.6rem; }
.bronnen a { font-size:.78rem; background:var(--bg); border:1px solid var(--line);
  border-radius:999px; padding:.15rem .55rem; text-decoration:none; color:var(--body); }

.how { margin-top:2rem; padding-top:1rem; border-top:1px solid var(--line); font-size:.85rem;
  color:var(--muted); max-width:44rem; line-height:1.5; }

.datalayer { display:none; gap:.35rem; flex-wrap:wrap; margin-top:.7rem; padding-top:.6rem;
  border-top:1px dashed var(--line); font-size:.72rem; }
body.data .datalayer { display:flex; }
.datalayer span { background:var(--ink); color:#e4e4e4; border-radius:.3rem; padding:.12rem .45rem; }
.datalayer b { color:#fff; }

.keep { position:fixed; left:0; right:0; bottom:0; background:var(--white);
  border-top:1px solid var(--line); z-index:8; box-shadow:0 -4px 18px rgb(39 39 39 / 6%); }
.keep-inner { max-width:var(--wrap); margin:0 auto; padding:.7rem 1.25rem; display:flex;
  align-items:center; gap:.85rem; flex-wrap:wrap; }
.keep img { width:2.6rem; height:2.6rem; object-fit:contain; background:var(--bg);
  border-radius:.3rem; }
.keep .who { font-size:.72rem; font-weight:800; letter-spacing:.08em; text-transform:uppercase;
  color:var(--primary); }
.keep .nm { font-weight:800; color:var(--ink); font-size:.95rem; }
.keep .price { font-size:1.15rem; margin-left:auto; }
.keep .btn { min-height:2.4rem; }

@media (max-width:800px) {
  .hero-grid { grid-template-columns:1fr; }
  .hero-grid img { max-width:16rem; margin:0 auto; }
  .alts { grid-template-columns:1fr; }
  .row { grid-template-columns:1.5rem 3.5rem 1fr; }
  .row .buy { grid-column:2 / -1; flex-direction:row; justify-content:space-between;
    align-items:center; text-align:left; }
  .keep .price { margin-left:0; }
}
@media (max-width:520px) {
  .alt { grid-template-columns:1fr; }
  .alt img { grid-row:auto; width:7rem; height:7rem; }
  .chips { flex-direction:column; }
  .chip { justify-content:center; }
}
@media print { .keep, .chooser, .mockbar { display:none !important; } }
"""

TICK = ('<svg class="tick" viewBox="0 0 20 20" fill="none" stroke="currentColor" '
        'stroke-width="2.4" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">'
        '<path d="M4 10.5l4 4 8-9"/></svg>')

PLACEHOLDER = ("data:image/svg+xml;utf8,"
               "<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 100 100'>"
               "<rect width='100' height='100' fill='%23f0f0f0'/>"
               "<text x='50' y='54' font-size='9' fill='%239a9a9a' text-anchor='middle'"
               " font-family='sans-serif'>geen foto</text></svg>")
