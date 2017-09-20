/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.microsoft.spark.streaming.modules.workloads

import com.microsoft.azure.powerbi.authentication.PowerBIAuthentication
import com.microsoft.azure.powerbi.common.{PowerBIDataTypes, PowerBIOptions, PowerBIURLs, PowerBIUtils}
import com.microsoft.azure.powerbi.models.table

object PowerBIPushData {

  val powerbiClientId = "client id"
  val powerbiAccountUsername = "username"
  val powerbiAccountPassword = "password"

  def initializeAuthentication(): PowerBIAuthentication = {

    val powerBIAuthentication: PowerBIAuthentication = new PowerBIAuthentication(
      PowerBIURLs.Authority,
      PowerBIURLs.Resource,
      powerbiClientId,
      powerbiAccountUsername,
      powerbiAccountPassword
    )

    powerBIAuthentication
  }

  def main(args: Array[String]): Unit = {

    val powerbiAuthentication: PowerBIAuthentication = initializeAuthentication()

    //Create PowerBI dataset and table

    val powerbiTableName = "CircleRadiusAreaTable"
    val powerbiDatasetName = "CircleDataset"

    val circleTableColumns = Map("Radius" -> PowerBIDataTypes.Double.toString(), "Area" -> PowerBIDataTypes.Double.toString())

    val circleTable = PowerBIUtils.defineTable(powerbiTableName, circleTableColumns)

    val powerbiTableList = List[table](circleTable)

    val powerbiDatasetDetails = PowerBIUtils.getOrCreateDataset(powerbiDatasetName, powerbiTableList,
      PowerBIOptions.basicFIFO, powerbiAuthentication.getAccessToken)

    println(powerbiDatasetDetails)
  }
}


