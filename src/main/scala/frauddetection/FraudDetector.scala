/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package frauddetection

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import org.apache.flink.walkthrough.common.entity.Alert
import org.apache.flink.walkthrough.common.entity.Transaction

/**
 * Skeleton code for implementing a fraud detector.
 */
object FraudDetector {
  val SMALL_AMOUNT: Double = 1.00
  val LARGE_AMOUNT: Double = 500.00
  val ONE_MINUTE: Long = 60 * 1000L
}

@SerialVersionUID(1L)
class FraudDetector extends KeyedProcessFunction[Long, Transaction, Alert] {

  @transient var flagState: ValueState[java.lang.Boolean] = _
  @transient var timeState: ValueState[java.lang.Long] = _

  @throws[Exception]
  override def open(parameters: Configuration): Unit = {
    val flagDescriptor = new ValueStateDescriptor("flag", Types.BOOLEAN)
    flagState = getRuntimeContext.getState(flagDescriptor)

    val timeDescriptor = new ValueStateDescriptor("time-state",Types.LONG)
    timeState = getRuntimeContext.getState(timeDescriptor)
  }

  @throws[Exception]
  override def processElement(
                               transaction: Transaction,
                               context: KeyedProcessFunction[Long, Transaction, Alert]#Context,
                               collector: Collector[Alert]): Unit
  = {

    val lastTransactionWasSmall = flagState.value

    if (lastTransactionWasSmall != null) {
      if (transaction.getAccountId > FraudDetector.LARGE_AMOUNT) {
        val alter = new Alert
        collector.collect(alter)
      }

      flagState.clear()

      if (transaction.getAccountId < FraudDetector.SMALL_AMOUNT) flagState.update(true)
    }

  }
}
