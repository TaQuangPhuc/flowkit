import json
from agent.services import video_evidence as ve
from agent.services.flow_trace import summary
from agent.services.flow_batch import RpcError


def test_shape_retains_rpc_code_uuid_not_secrets():
    uuid = 'd5129819-0bff-4460-a7a9-69b8cc8ba59a'
    output = json.dumps(ve.shape([uuid, 5, 'Bearer secret', 'https://host/video/x?token=abc', {'private-prompt':'secret'}]))
    assert uuid in output and '5' in output
    assert all(x not in output for x in ['Bearer', 'secret', 'token=', 'private-prompt'])
    assert summary(RpcError('as29s', [5]))['rpc_codes'] == [5]


def test_transition_only_on_state_change(monkeypatch):
    ve._states.clear()
    events=[]
    monkeypatch.setattr(ve, 'emit', lambda *a, **k: events.append(k))
    for state in ['no_urls', 'no_urls', 'rpc_error:5', 'rpc_error:5', 'video']:
        ve.transition('media','uuid',state,payload=[None,5])
    assert len(events)==3
    assert events[1]['previous']=='no_urls'
    assert events[2]['state']=='video'


def test_structure_bounded():
    assert len(json.dumps(ve.shape([[['secret']*100]*100]*100))) < 15000
