import math,hashlib
import numpy as np
from sklearn.cluster import DBSCAN
from sklearn.model_selection import train_test_split
from sklearn.metrics import adjusted_rand_score

def compare(baseline,assignments):
    """逐点比较正式识别与候选参数：标出在鱼群与噪点之间改变的点。"""
    before={p['echo_id']:p['label'] for p in baseline or []}
    changed=[]
    for p in assignments:
        if p['echo_id'] not in before:continue
        was_noise=before[p['echo_id']]<0;now_noise=p['label']<0
        if was_noise!=now_noise:changed.append({'echo_id':p['echo_id'],'before':before[p['echo_id']],'after':p['label'],'change':'to_noise' if now_noise else 'to_fish'})
    fish_to_noise=sum(1 for c in changed if c['change']=='to_noise')
    return {'changed':changed,'changed_count':len(changed),'fish_to_noise':fish_to_noise,'noise_to_fish':len(changed)-fish_to_noise}

def analyze(points,eps,min_samples):
    lon0=points[0]['lon'];lat0=points[0]['lat']
    x=np.array([[math.radians(p['lon']-lon0)*6371000*math.cos(math.radians(lat0))/60,math.radians(p['lat']-lat0)*6371000/60,p['depth_m']/3,p['strength_db']/8] for p in points])
    labels=DBSCAN(eps=eps,min_samples=min_samples).fit_predict(x)
    rng=np.random.default_rng(37);ref=np.vstack([rng.normal(c,.16,(35,4)) for c in [[0,0,2,-5],[4,0,5,-4],[8,0,8,-7]]]+[rng.uniform([-1,-1,0,-12],[10,1,10,-2],(15,4))]);truth=np.repeat([0,1,2,-1],[35,35,35,15])
    train,test=train_test_split(np.arange(120),test_size=.3,random_state=37,stratify=truth)
    sensitivity=[]
    for e in [.8,1.2,1.6]:
        observed=DBSCAN(eps=e,min_samples=min_samples).fit_predict(x);held=DBSCAN(eps=e,min_samples=3).fit_predict(ref[test]);sensitivity.append({'eps':e,'clusters':len(set(observed)-{-1}),'noise':int(sum(observed==-1)),'reference_holdout_ari':float(adjusted_rand_score(truth[test],held))})
    hold=DBSCAN(eps=eps,min_samples=3).fit_predict(ref[test])
    return [{'echo_id':p['id'],'label':int(label)} for p,label in zip(points,labels)],{'clusters':len(set(labels)-{-1}),'noise_count':int(sum(labels==-1)),'point_count':len(points),'reference_holdout_ari':float(adjusted_rand_score(truth[test],hold)),'sensitivity':sensitivity,'reference_train_count':len(train),'reference_holdout_count':len(test),'seed':37,'input_sha256':hashlib.sha256(x.tobytes()).hexdigest(),'source':'实际回波聚类；ARI为独立合成参照的36点留出验证，无监督算法不训练预测模型'}
