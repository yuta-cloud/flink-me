/*
 *
 *
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 *
 *
 *
 */

package org.apache.flink.runtime.causal.determinant;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.Condition;

public class AckReceiver {

    private final Lock lock = new ReentrantLock();
    private final Condition receivedAck = lock.newCondition();
    private boolean ackReceived = false;
    private int waitCount = 0;
    private int ackCount = 0;
    private int vertexID;
    private final int firstCount = 10000;

    public void waitForAck(short id) throws InterruptedException {
        this.vertexID = (int) id;
        waitCount++;
        System.out.println("wait ack: " + waitCount + " id: " + id);
        if(waitCount < firstCount)
            return;
        lock.lock();
        try {
            while (!ackReceived) {
                receivedAck.await();
            }
            System.out.println("release lock: " + waitCount + " id: " + id);
            ackReceived = false;
        } finally {
            lock.unlock();
        }
    }

    public void receiveAck(int id) {
        //System.out.println("receive ack: " + ackCount + " id: " + id + " thisID: " + vertexID);
        if(this.vertexID != id)
            return;
        ackCount++;
        //System.out.println("receive2 ack: " + waitCount + " id: " + id);
        if(ackCount < firstCount)
            return;
        lock.lock();
        try {
            ackReceived = true;
            receivedAck.signalAll();
        } finally {
            lock.unlock();
        }
    }
}