/*
 * Copyright 2017 Saxon State and University Library Dresden (SLUB)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package de.qucosa.util;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Utility class to run a TerminatableRunnable object.
 */
public class TerminateableRunner {

    /**
     * Execute the given runnable in a thread and wait for it to finish, or until the timeout strikes.
     *
     * @param runnable            A runnable object to execute
     * @param timeoutMilliseconds Number of milliseconds before interrupting the execution
     * @throws InterruptedException Thrown when the runnable is interrupted
     */
    public static void runAndWait(TerminateableRunnable runnable, long timeoutMilliseconds) throws InterruptedException {
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.execute(runnable);

        // FIXME A bit of a hack
        runnable.terminate(); // immediately request runner termination

        executorService.shutdown(); // immediately request executor service shutdown
        executorService.awaitTermination(timeoutMilliseconds, TimeUnit.MILLISECONDS);
    }

}
