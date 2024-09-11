package org.apache.celeborn.service.deploy.master.http.api

import javax.ws.rs.BadRequestException

import org.mockito.MockitoSugar._

import org.apache.celeborn.CelebornFunSuite
import org.apache.celeborn.service.deploy.master.Master

class TestMasterHttpResourceUtils extends CelebornFunSuite {

  test("ensureMasterIsLeader") {
    val mockMaster = mock[Master]
    when(mockMaster.isMasterActive).thenReturn(0)
    assertThrows[BadRequestException] {
      MasterHttpResourceUtils.ensureMasterIsLeader(mockMaster) {
        "operation failed"
      }
    }
  }
}
