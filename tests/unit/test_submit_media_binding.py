from unittest.mock import AsyncMock
import pytest
from agent.services import flow_batch as fb
from agent.services.flow_client import FlowClient

OP='220d0813-31a7-438d-9de8-7a7ffbf5f122'
MID='7fc47445-c3eb-49cb-b2bf-37e7087aca39'
PID='9fd3eefc-cbda-443a-a999-4ff16002a419'


def ack():
    return [None,5954,[[OP,None,None,['title',[1,2],None,None,MID,'client',[1,3]],PID]],[[MID,PID,OP,'CAE']]]


def test_binds_only_matching_project_operation_and_media():
    assert fb.submitted_video_media(ack(),OP,PID)==MID
    for target in ['project','operation','media','missing']:
        data=ack()
        if target=='project':data[3][0][1]='other'
        if target=='operation':data[3][0][2]='other'
        if target=='media':data[3][0][0]='other'
        if target=='missing':data.pop()
        assert fb.submitted_video_media(data,OP,PID) is None


@pytest.mark.asyncio
async def test_ready_media_bypasses_broken_listing_on_refresh_round():
    c=FlowClient();c._operation_media[OP]=MID;c._operation_polls[OP]=2
    c._batch_media_urls=AsyncMock(return_value=fb.MediaUrls(MID,video='https://flow-content.google/video/test'))
    c._find_operation_media=AsyncMock(side_effect=TimeoutError('listing failed'))
    result=await c._poll_batch_operation_inner(OP)
    assert result['status']=='MEDIA_GENERATION_STATUS_SUCCESSFUL'
    c._find_operation_media.assert_not_awaited()
    c._batch_media_urls.assert_awaited_once_with(MID)
