"""单分组集的精确整行聚合慢路径。"""
from fractions import Fraction
from .typed import key

def group(rows,dimensions,aggregates):
    groups={}
    for row in rows:
        values=tuple(row[d] for d in dimensions);encoded=key(values)
        groups.setdefault(encoded,[values,[]])[1].append(row)
    if not dimensions and not groups:groups[key(())]=[(),[]]
    result=[]
    for values,members in groups.values():
        output={d:v for d,v in zip(dimensions,values)}
        for name,op,fields,distinct in aggregates:
            if op=='count_all':output[name]=len(members);continue
            inputs=[tuple(r[f] for f in fields) for r in members];inputs=[v for v in inputs if all(x is not None for x in v)]
            if distinct:inputs=list({key(v):v for v in inputs}.values())
            if op=='count':value=len(inputs)
            elif op=='sum':value=sum(v[0] for v in inputs) if inputs else None
            elif op=='avg':value=str(Fraction(sum(v[0] for v in inputs),len(inputs))) if inputs else None
            else:raise ValueError('聚合不支持')
            output[name]=value
        result.append(output)
    return result

def inspect(data,store,root):
    result=group(data['rows'],['team'],[('rows','count_all',[],False),('sum','sum',['amount'],False),('users','count',['user'],True)])
    return {'grouping':['team'],'rows':result,'source_rows':len(data['rows'])}
