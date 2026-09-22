import time
from types import SimpleNamespace
from unittest.mock import AsyncMock
import pytest
from agent.services.video_reconciliation import VideoReconciler


def setup_client(result):
    now=time.time()
    c=SimpleNamespace(_operation_results={'op':{'error_code':'upstream_timeout'}},
        _operation_start_time={'op':now-550}, _operation_profiles={'op':'nick'},
        _operation_projects={'op':'project'},
        workers=lambda:[{'profile_id':'nick','available':True,'in_flight':0}],
        _poll_batch_operation_inner=AsyncMock(return_value=result))
    async def route(builder,project,**kw):
        assert project=='project' and kw=={'profile_id':'nick','operation_id':'op','allow_failover':False}
        return await builder(project)
    c._run_on_profile=AsyncMock(side_effect=route)
    return c,now


@pytest.mark.asyncio
async def test_late_result_durable_without_overwriting_failure(tmp_path):
    c,now=setup_client({'status':'MEDIA_GENERATION_STATUS_SUCCESSFUL','operation':{'name':'op'}})
    r=VideoReconciler(c,tmp_path/'journal.json')
    await r.tick(now)
    assert r.rows['op']['state']=='late_succeeded'
    assert c._operation_results['op']=={'error_code':'upstream_timeout'}
    resumed=VideoReconciler(c,r.path)
    await resumed.tick(now+150)
    assert c._poll_batch_operation_inner.await_count==1
    assert r.path.stat().st_mode & 0o777 == 0o600


@pytest.mark.asyncio
async def test_backoff_survives_restart_and_expires(tmp_path):
    c,now=setup_client({'status':'MEDIA_GENERATION_STATUS_PENDING'})
    r=VideoReconciler(c,tmp_path/'journal.json')
    await r.tick(now)
    r=VideoReconciler(c,r.path)
    await r.tick(now+10)
    assert c._poll_batch_operation_inner.await_count==1
    await r.tick(now+1900)
    assert r.rows['op']['state']=='expired'
    assert c._poll_batch_operation_inner.await_count==1


@pytest.mark.asyncio
async def test_skip_busy_unbound_and_non_timeout(tmp_path):
    c,now=setup_client({})
    c.workers=lambda:[{'profile_id':'nick','available':True,'in_flight':1}]
    r=VideoReconciler(c,tmp_path/'journal.json');await r.tick(now)
    c.workers=lambda:[{'profile_id':'nick','available':True,'in_flight':0}]
    c._operation_profiles.clear();await r.tick(now)
    c._operation_profiles['op']='nick'
    c._operation_results['op']={'error_code':'content_policy_violation'};await r.tick(now)
    c._poll_batch_operation_inner.assert_not_awaited()


@pytest.mark.asyncio
async def test_network_failure_keeps_original_job_and_backoff(tmp_path):
    c,now=setup_client({});c._poll_batch_operation_inner.side_effect=TimeoutError()
    r=VideoReconciler(c,tmp_path/'journal.json');await r.tick(now)
    assert r.rows['op']['state']=='pending'
    assert r.rows['op']['next_check_at']==now+120
    assert c._operation_results['op']['error_code']=='upstream_timeout'
