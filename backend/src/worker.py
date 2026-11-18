import logging,time,json,os
from pathlib import Path
from sqlalchemy import select
from .database import Session
from .models import SchoolAnalysis,SchoolTrial,SchoolTrialRun,Transect,Echo
from .school_service import queue,refresh_trial
from .algorithm import analyze,compare
from .common import dump
logging.basicConfig(level=logging.INFO)
CHANNELS=['estuary:schools','estuary:school-trials']
ARTIFACTS=Path(os.getenv('ARTIFACTS_DIR','/artifacts'))

def trial_points(db,run):
    trial=db.get(SchoolTrial,run.trial_id);t=db.get(Transect,trial.transect_id)
    if trial.data_version!=t.data_version:raise ValueError('回波已更新，请重新试算')
    base=db.get(SchoolAnalysis,trial.baseline_analysis_id)
    if base is None or base.data_version!=t.data_version:raise ValueError('正式结果已更换，请重新试算')
    return trial,base,[dump(p) for p in db.scalars(select(Echo).where(Echo.transect_id==t.id).order_by(Echo.id))]

def run_trial(db,run):
    trial,base,points=trial_points(db,run)
    run.assignments,run.metrics=analyze(points,run.eps,run.min_samples)
    run.metrics['baseline_analysis_id']=base.id
    run.metrics['baseline_eps']=base.eps
    run.metrics['baseline_min_samples']=base.min_samples
    run.metrics['comparison']=compare(base.assignments,run.assignments)
    run.status='done'
    try:
        ARTIFACTS.mkdir(parents=True,exist_ok=True);(ARTIFACTS/f'school-trial-{run.id}.json').write_text(json.dumps({'metrics':run.metrics,'comparison':run.metrics['comparison']},ensure_ascii=False))
    except OSError:logging.exception('写入试算产物失败，结果已保存数据库')
    refresh_trial(db,trial)

def pending(run_id):
    return queue.lpos('estuary:school-trials',f'trial:{run_id}') is not None

def recover():
    """worker重启时把中断或已丢失队列消息的试算组合记为失败，供主管续跑；已完成组合不受影响。"""
    with Session() as db:
        for run in db.scalars(select(SchoolTrialRun).where(SchoolTrialRun.status.in_(['queued','running']))):
            if run.status=='running' or not pending(run.id):
                run.status='failed';run.error='worker中断或队列消息丢失，可续跑该组合'
        for trial in db.scalars(select(SchoolTrial).where(SchoolTrial.status.in_(['queued','running']))):
            refresh_trial(db,trial)
        db.commit()

def handle(item):
    kind,ident=item.split(':',1)
    with Session() as db:
        if kind=='analysis':
            row=db.get(SchoolAnalysis,int(ident))
            if not row or row.status!='queued':return
            row.status='running';db.commit()
            try:
                t=db.get(Transect,row.transect_id)
                if row.data_version!=t.data_version:raise ValueError('回波已更新，请重新提交最新版本')
                points=[dump(p) for p in db.scalars(select(Echo).where(Echo.transect_id==t.id).order_by(Echo.id))]
                row.assignments,row.metrics=analyze(points,row.eps,row.min_samples);row.status='done'
                ARTIFACTS.mkdir(parents=True,exist_ok=True);(ARTIFACTS/f'school-{row.id}.json').write_text(json.dumps(row.metrics,ensure_ascii=False))
            except Exception as e:row.status='failed';row.error=str(e)[:200]
            db.commit()
        elif kind=='trial':
            run=db.get(SchoolTrialRun,int(ident))
            if not run or run.status!='queued':return
            run.status='running';db.commit()
            try:run_trial(db,run)
            except Exception as e:
                run.status='failed';run.error=str(e)[:200]
                trial=db.get(SchoolTrial,run.trial_id)
                if trial:refresh_trial(db,trial)
            db.commit()

def main():
    while True:
        try:recover();break
        except Exception:logging.exception('worker恢复中断试算失败，稍后重试');time.sleep(2)
    while True:
        Path('/tmp/worker-heartbeat').touch()
        try:
            item=queue.blpop(CHANNELS,timeout=2)
            if not item:continue
            value=item[1]
            # 兼容旧队列中仅含裸分析ID的消息
            handle(value if ':' in value else f'analysis:{value}')
        except Exception:logging.exception('echo worker');time.sleep(1)

if __name__=='__main__':main()
