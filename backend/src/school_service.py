import os,redis
from fastapi import HTTPException
from sqlalchemy import select,func
from .models import SchoolAnalysis,SchoolReview,Transect,Echo,SchoolTrial,SchoolTrialRun
from .common import get,reject,finish,transect_lock
queue=redis.Redis.from_url(os.getenv('REDIS_URL','redis://redis:6379/0'),decode_responses=True)
def latest(db,transect_id,reviewed=False):
    t=get(db,Transect,transect_id)
    row=db.scalar(select(SchoolAnalysis).where(SchoolAnalysis.transect_id==transect_id,SchoolAnalysis.data_version==t.data_version).order_by(SchoolAnalysis.id.desc()))
    if row is None or row.status not in (('reviewed',) if reviewed else ('done','reviewed')):return None
    return row
def enqueue(db,data,user):
    c,t=transect_lock(db,data.transect_id)
    reject(db.scalar(select(func.count()).select_from(Echo).where(Echo.transect_id==t.id))<8,'至少8个回波点才可识别群落')
    reject(db.scalar(select(SchoolAnalysis).where(SchoolAnalysis.transect_id==t.id,SchoolAnalysis.data_version==t.data_version,SchoolAnalysis.status.in_(['queued','running']))) is not None,'当前版本正在识别')
    row=SchoolAnalysis(**data.model_dump(),data_version=t.data_version);db.add(row);db.flush();result=finish(db,user,'提交回波群落识别',row)
    try:queue.rpush('estuary:schools',row.id)
    except redis.RedisError:
        row.status='failed';row.error='队列暂不可用，恢复后可重新识别';db.commit();raise HTTPException(503,row.error)
    return result
def review(db,id,data,user):
    initial=get(db,SchoolAnalysis,id);c,t=transect_lock(db,initial.transect_id);row=get(db,SchoolAnalysis,id,True)
    reject(row.status!='done' or row.data_version!=t.data_version,'仅最新回波版本的已完成识别可复核')
    reject(row.metrics.get('noise_count',0)>0 and len(data.noise_note.strip())<8,'存在噪点，请补充至少8字现场解释')
    row.status='reviewed';t.status='reviewed';review=SchoolReview(analysis_id=row.id,**data.model_dump());db.add(review);db.flush();return finish(db,user,'群落与噪点复核',review)

def formal(db,transect_id,version):
    """该回波版本当前的正式识别：已完成/已复核中编号最大者（失败的尝试不算正式结果）。"""
    return db.scalar(select(SchoolAnalysis).where(SchoolAnalysis.transect_id==transect_id,SchoolAnalysis.data_version==version,SchoolAnalysis.status.in_(['done','reviewed'])).order_by(SchoolAnalysis.id.desc()).limit(1))
def refresh_trial(db,trial):
    if trial.adopted_analysis_id:return
    statuses=set(db.scalars(select(SchoolTrialRun.status).where(SchoolTrialRun.trial_id==trial.id)))
    if 'running' in statuses:trial.status='running'
    elif 'queued' in statuses:trial.status='queued'
    elif statuses=={'done'}:trial.status='done'
    else:trial.status='failed'

def _push(db,runs):
    """把组合送入队列；发送失败的组合标记failed，可续跑且不会重算已完成的组合。"""
    failed=False
    for r in runs:
        try:queue.rpush('estuary:school-trials',f'trial:{r.id}')
        except redis.RedisError:r.status='failed';r.error='队列暂不可用，可续跑失败组合';failed=True
    return failed

def start_trial(db,data,user):
    c,t=transect_lock(db,data.transect_id)
    reject(db.scalar(select(func.count()).select_from(Echo).where(Echo.transect_id==t.id))<8,'至少8个回波点才可试算候选参数')
    pairs=[(round(x.eps,3),x.min_samples) for x in data.candidates]
    reject(len(set(pairs))!=len(pairs),'候选参数组合不能重复')
    base=formal(db,t.id,t.data_version);reject(base is None,'最新回波版本尚无可用正式识别结果，先完成正式识别')
    trial=SchoolTrial(transect_id=t.id,data_version=t.data_version,baseline_analysis_id=base.id);db.add(trial);db.flush()
    runs=[SchoolTrialRun(trial_id=trial.id,eps=eps,min_samples=min_samples) for eps,min_samples in pairs];db.add_all(runs);db.flush()
    result=finish(db,user,'提交候选参数试算',trial,f'试算#{trial.id} 航段{t.code} v{t.data_version} 正式识别#{base.id} 组合{len(runs)}个')
    if _push(db,runs):
        refresh_trial(db,trial);db.commit();raise HTTPException(503,'队列暂不可用，试算组合已保留，恢复后续跑失败组合')
    return result

def resume_trial(db,id,user):
    initial=get(db,SchoolTrial,id);c,t=transect_lock(db,initial.transect_id);trial=get(db,SchoolTrial,id,True)
    reject(trial.adopted_analysis_id is not None,'试算已采纳，不能续跑')
    reject(trial.data_version!=t.data_version,'回波已补录，请重新试算后再采纳')
    current=formal(db,t.id,t.data_version);reject(current is None or current.id!=trial.baseline_analysis_id,'正式结果已更换，请重新试算')
    runs=list(db.scalars(select(SchoolTrialRun).where(SchoolTrialRun.trial_id==trial.id,SchoolTrialRun.status=='failed')))
    reject(not runs,'没有失败的试算组合可续跑')
    for r in runs:r.status='queued';r.error=''
    refresh_trial(db,trial)
    result=finish(db,user,'续跑候选参数试算',trial,f'试算#{trial.id} 续跑组合#{",".join(str(r.id) for r in runs)}')
    if _push(db,runs):
        refresh_trial(db,trial);db.commit();raise HTTPException(503,'队列暂不可用，失败组合已保留，恢复后再次续跑')
    return result

def adopt(db,id,data,user):
    initial=get(db,SchoolTrial,id);c,t=transect_lock(db,initial.transect_id);trial=get(db,SchoolTrial,id,True)
    reject(trial.adopted_analysis_id is not None,'该试算已被采纳，以先成功的采纳为准')
    run=get(db,SchoolTrialRun,data.run_id,True);reject(run.trial_id!=trial.id,'候选组合不属于该试算')
    reject(run.status!='done','该候选组合尚未试算成功，不能采纳')
    reject(db.scalar(select(SchoolTrialRun).where(SchoolTrialRun.trial_id==trial.id,SchoolTrialRun.status.in_(['queued','running']))) is not None,'试算尚未结束，请等待组合完成或续跑失败组合')
    reject(trial.data_version!=t.data_version,'回波已补录，请重新试算后再采纳')
    current=formal(db,t.id,t.data_version);reject(current is None or current.id!=trial.baseline_analysis_id,'正式结果已更换，请重新试算后再采纳')
    reject(db.scalar(select(SchoolAnalysis).where(SchoolAnalysis.transect_id==t.id,SchoolAnalysis.data_version==t.data_version,SchoolAnalysis.status.in_(['queued','running']))) is not None,'已有新的正式识别在途，请等待完成后再采纳')
    metrics=dict(run.metrics or {});metrics['source']='候选参数试算采纳：'+(metrics.get('source') or '');metrics['adopted_from_trial']=trial.id;metrics['adopted_from_run']=run.id
    row=SchoolAnalysis(transect_id=t.id,data_version=t.data_version,eps=run.eps,min_samples=run.min_samples,status='done',metrics=metrics,assignments=run.assignments)
    db.add(row);db.flush()
    trial.status='adopted';trial.adopted_analysis_id=row.id;trial.adopted_by=user.username
    return finish(db,user,'采纳候选参数并替换正式结果',row,f'试算#{trial.id} 组合#{run.id} eps{run.eps}/min{run.min_samples} -> 正式识别#{row.id}')
