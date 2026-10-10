import re,sys
txt=open(sys.argv[1]).read()
states=re.split(r'\nState (\d+): ',txt)
keys=['status','oWallet','oTotal','balance','pc','captured','charge','apc','aSnapW','otherSpent','flagged']
for i in range(1,len(states),2):
    body=states[i+1]
    act=body.split('\n',1)[0]
    act=re.sub(r' line.*','',act).strip('<> ')
    vals={}
    for k in keys:
        m=re.search(r'/\\ '+k+r' = (.*)',body)
        if m: vals[k]=m.group(1)
    print(f"{states[i]:>2} {act:<16}", ' '.join(f"{k}={vals[k]}" for k in keys if k in vals))
