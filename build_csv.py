import json,glob,re,csv,unicodedata,html
from datetime import datetime,timezone
from collections import Counter
LEAD=re.compile(r'^(?:\s*@\w+)+\s*')
PAT={'Orange':r'\borange(?:_france|_conseil|_sav)?\b|\blivebox\b|\bsosh\b',
 'Free':r'free\s*mobile|\bfreebox|@free\b|@freebox|\bchez free\b|\bfree\b(?=.{0,40}\b(?:box|fibre|forfait|réseau|opérateur|abonn))|(?:opérateur|box|fibre|forfait|abonn\w*|client)\s.{0,20}\bfree\b',
 'SFR':r'\bsfr(?:_sav)?\b|red by sfr',
 'Bouygues Telecom':r'\bbouygues\b|@bouyguestelecom|\bbbox\b|b&you'}
def ops(t):
    body=LEAD.sub('',html.unescape(t)).lower()
    return [o for o,p in PAT.items() if re.search(p,body)]
def norm(t):
    t=LEAD.sub('',unicodedata.normalize('NFKC',html.unescape(t)).lower())
    t=re.sub(r'https?://\S+|@\w+','',t); return re.sub(r'\W+',' ',t).strip()
now=datetime.now(timezone.utc).isoformat()
rows={};seen=set();c=Counter()
for f in sorted(glob.glob('/workspace/agent-tools/*.txt')):
    s=open(f).read(); i=s.find('{"data"')
    if i<0: continue
    d=json.loads(s[i:s.rfind('}')+1])
    for p in d.get('data',[]):
        if p['id'] in rows: continue
        o=ops(p['text']); n=norm(p['text'])
        if not o or n in seen or len(n.split())<4: c['rejet']+=1; continue
        seen.add(n); t=p['text'].rstrip()
        trunc=t.endswith(('…','...'))
        rows[p['id']]=dict(post_id=p['id'],post_url=p.get('url',''),text=p['text'],created_at=p.get('created_at',''),
          collected_at=now,lang=p.get('lang',''),operators='|'.join(o),like_count=p.get('public_metrics',{}).get('like_count',0),
          text_complete='false' if trunc else 'true')
CAP=55
cnt=Counter(o for r in rows.values() for o in r['operators'].split('|'))
for k in sorted([k for k,r in rows.items() if r['operators']=='Orange'],key=lambda k:-int(k)):
    if cnt['Orange']<=CAP: break
    del rows[k]; cnt['Orange']-=1
for r in rows.values():
    for o in r['operators'].split('|'): c[o]+=1
with open('/workspace/telecom/telecom_equilibre.csv','w',newline='',encoding='utf-8') as h:
    w=csv.DictWriter(h,fieldnames=list(next(iter(rows.values())).keys()));w.writeheader();w.writerows(rows.values())
print(len(rows),c)
