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

package io.connect.paimon.coordinator;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/** A coordinator. */
public class Coordinator {
    private static final List<CoordinatorTask> tasks = new ArrayList<>();
    private static final ScheduledExecutorService executorService =
            Executors.newScheduledThreadPool(1);

    public static void registry(CoordinatorTask task) {
        tasks.add(task);
    }

    // Start schedule task.
    public static void start() {
        for (CoordinatorTask task : tasks) {
            executorService.scheduleAtFixedRate(
                    task, 1000, task.getIntervalMs(), TimeUnit.MILLISECONDS);
        }
    }
}
