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

package org.apache.flink.runtime.messages

import java.util.UUID

import org.apache.flink.runtime.clusterframework.types.ResourceID
import org.apache.flink.runtime.instance.{HardwareDescription, InstanceID}
import org.apache.flink.runtime.taskmanager.TaskManagerLocation

import scala.concurrent.duration.{Deadline, FiniteDuration}

/**
 * A set of messages between TaskManager and JobManager to handle the
 * registration of the TaskManager at the JobManager.
  * TaskManager 和 JobManager 之间的消息集合, 用来处理 TaskManager 向 JobManager 的注册行为。
 */
object RegistrationMessages {

  /**
   * Marker trait for registration messages.
    * 用于注册消息的标识特质。
   */
  trait RegistrationMessage extends RequiresLeaderSessionID {}

  /**
   * Triggers the TaskManager to attempt a registration at the JobManager.
    * 触发 TaskManager 尝试向 JobManager 注册。
   *
   * @param jobManagerURL Akka URL to the JobManager
    *                      JobManager 的akka url
   * @param timeout The timeout for the message. The next retry will double this timeout.
    *                消息的超时时间 。 下次尝试会将超时时间翻倍。
   * @param deadline Optional deadline until when the registration must be completed.
    *                 注册必须完成的截止时间, 可选项。
   * @param attempt The attempt number, for logging.
    *                尝试次数, 用于 log
   * @param registrationRun UUID of the current registration run to filter out outdated runs
    *                        当前注册运行的 UUID , 用来过滤过时的运行。
   */
  case class TriggerTaskManagerRegistration(
      jobManagerURL: String,
      timeout: FiniteDuration,
      deadline: Option[Deadline],
      attempt: Int,
      registrationRun: UUID)
    extends RegistrationMessage

  /**
   * Registers a task manager at the JobManager. A successful registration is acknowledged by
   * [[AcknowledgeRegistration]].
    * 向 JobManager 注册一个 task manager 。
    * 一个成功的注册会被回复 [[AcknowledgeRegistration]] 。
   *
   * @param connectionInfo The TaskManagers connection information.
   * @param resources The TaskManagers resources.
   * @param numberOfSlots The number of processing slots offered by the TaskManager.
   */
  case class RegisterTaskManager(
                                  resourceId: ResourceID,
                                  connectionInfo: TaskManagerLocation,
                                  resources: HardwareDescription,
                                  numberOfSlots: Int)
    extends RegistrationMessage

  /**
   * Denotes the successful registration of a task manager at the JobManager. This is the
   * response triggered by the [[RegisterTaskManager]] message when the JobManager has registered
   * the task manager with the resource manager.
    * 标识一个task manager 成功注册到了 JobManager 上。
    * 当JobManager将 task manager 注册到了 resouce manager上后,会回复该消息。
   *
   * @param instanceID The instance ID under which the TaskManager is registered at the
   *                   JobManager.
    *                   TaskManager 注册到的 JobManager 的实例ID。
   * @param blobPort The server port where the JobManager's BLOB service runs.
    *                 JobManager 的 BLOB 服务运行的服务端口
   */
  case class AcknowledgeRegistration(
      instanceID: InstanceID,
      blobPort: Int)
    extends RegistrationMessage

  /**
   * Denotes that the TaskManager has already been registered at the JobManager.
    * 标识 TaskManager 之前已经被注册到 JobManager 上了。
   *
   * @param instanceID The instance ID under which the TaskManager is registered.
   * @param blobPort The server port where the JobManager's BLOB service runs.
   */
  case class AlreadyRegistered(
      instanceID: InstanceID,
      blobPort: Int)
    extends RegistrationMessage

  /**
   * Denotes the unsuccessful registration of a task manager at the JobManager. This is the
   * response triggered by the [[RegisterTaskManager]] message.
    * 标识一个 TaskManager 向 JobManager 注册不成功。
    * 这是由 [[RegisterTaskManager]] 消息触发的回复。
   *
   * @param reason Reason why the task manager registration was refused
    *               task manager 注册为什么被拒绝的原因。
   */
  case class RefuseRegistration(reason: Throwable) extends RegistrationMessage

}
